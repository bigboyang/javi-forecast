"""Forecast orchestrator.

Runs as a background asyncio task.  Every ``FORECAST_INTERVAL_SECONDS``
it iterates over all services × RED metric dimensions, picks the best
model via the selector, stores results in the ForecastStore, and fires
anomaly alerts via the WebhookAlerter.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import numpy as np

from ..config import settings
from ..models.forecast import ForecastResult, ModelType, PredictionPoint
from ..anomaly.predictor import AnomalyPredictor
from ..anomaly.isolation_forest import IsolationForestDetector
from ..alerter.webhook import WebhookAlerter
from .baseline_store import BaselineStore
from .feature_store import FeatureStore
from .metric_feature_store import MetricFeatureStore
from .selector import select_model
from ..store.forecast_store import ForecastStore

logger = logging.getLogger(__name__)

_METRIC_NAMES = ("rate", "error_rate", "p50_ms", "p95_ms", "p99_ms")

# Prometheus instruments (lazy-init)
_run_hist = None
_mse_gauge = None
_anomaly_counter = None
_store_gauge = None


def _init_metrics():
    global _run_hist, _mse_gauge, _anomaly_counter, _store_gauge
    if _run_hist is not None:
        return
    try:
        from prometheus_client import Histogram, Gauge, Counter
        _run_hist = Histogram(
            "javi_forecast_run_duration_seconds",
            "Time spent running a forecast cycle",
        )
        _mse_gauge = Gauge(
            "javi_forecast_model_mse",
            "Forecast model MSE",
            ["service", "metric", "model"],
        )
        _anomaly_counter = Counter(
            "javi_forecast_anomalies_predicted_total",
            "Total predicted anomalies",
            ["service", "severity"],
        )
        _store_gauge = Gauge(
            "javi_forecast_feature_store_size",
            "Number of data points in the feature store",
            ["service"],
        )
    except Exception:
        pass


class Forecaster:
    """Background forecast scheduler.

    Parameters
    ----------
    feature_store:
        Source of historical RED metrics.
    forecast_store:
        Destination for computed ForecastResult objects.
    anomaly_predictor:
        Used to classify predicted anomalies.
    alerter:
        Fires webhooks when anomalies are predicted.
    interval_seconds:
        How often to run the forecast loop.
    horizon_minutes:
        How far into the future to predict.
    window_minutes:
        How much history to use as input features.
    min_data_points:
        Minimum number of history points before a forecast is attempted.
    default_model:
        Model name passed to the selector.
    """

    def __init__(
        self,
        feature_store: FeatureStore,
        forecast_store: ForecastStore,
        anomaly_predictor: AnomalyPredictor,
        alerter: WebhookAlerter,
        metric_feature_store: Optional[MetricFeatureStore] = None,
        interval_seconds: int = settings.FORECAST_INTERVAL_SECONDS,
        horizon_minutes: int = settings.FORECAST_HORIZON_MINUTES,
        window_minutes: int = settings.FEATURE_WINDOW_MINUTES,
        min_data_points: int = settings.MIN_DATA_POINTS,
        default_model: str = settings.DEFAULT_MODEL,
    ) -> None:
        self._feature_store = feature_store
        self._metric_feature_store = metric_feature_store
        self._forecast_store = forecast_store
        self._predictor = anomaly_predictor
        self._alerter = alerter
        self.interval_seconds = interval_seconds
        self.horizon_minutes = horizon_minutes
        self.window_minutes = window_minutes
        self.min_data_points = min_data_points
        self.default_model = default_model

        # P2-A: hour-of-week baseline for Z-score normalisation
        self._baseline_store = BaselineStore()
        # P2-C: multivariate Isolation Forest detector
        self._iso_forest = IsolationForestDetector(
            min_samples=settings.ISO_FOREST_MIN_SAMPLES,
        )

        self._task: Optional[asyncio.Task] = None
        self._running = False
        _init_metrics()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="forecaster")
        logger.info(
            "Forecaster started interval=%ds horizon=%dm model=%s",
            self.interval_seconds,
            self.horizon_minutes,
            self.default_model,
        )

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Forecaster stopped")

    # ------------------------------------------------------------------
    # Internal loop
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        while self._running:
            t0 = time.monotonic()
            try:
                await self._run_cycle()
            except Exception as exc:
                logger.error("Forecast cycle error: %s", exc, exc_info=True)
            elapsed = time.monotonic() - t0
            if _run_hist:
                _run_hist.observe(elapsed)
            sleep_for = max(0.0, self.interval_seconds - elapsed)
            await asyncio.sleep(sleep_for)

    async def _run_cycle(self) -> None:
        services = self._feature_store.get_services()
        if not services:
            logger.debug("Forecaster: no services in feature store yet")
            return

        logger.info("Forecast cycle: services=%d", len(services))
        tasks = [
            self._forecast_service_metric(service, metric)
            for service in services
            for metric in _METRIC_NAMES
        ]
        # Run concurrently but limit parallelism to avoid overwhelming CPU
        sem = asyncio.Semaphore(8)

        async def _bounded(coro):
            async with sem:
                return await coro

        await asyncio.gather(*[_bounded(t) for t in tasks], return_exceptions=True)

        # Update feature-store size gauge
        if _store_gauge:
            for service in services:
                _store_gauge.labels(service=service).set(
                    self._feature_store.size(service)
                )

        # Forecast custom OTel metrics from MetricFeatureStore
        if self._metric_feature_store is not None:
            await self._run_custom_metric_cycle()

        # P2-C: Isolation Forest multivariate anomaly detection
        await self._run_isolation_forest_cycle(services)

        # Evict stale forecasts
        await self._forecast_store.evict_expired()

    async def _run_isolation_forest_cycle(self, services: List[str]) -> None:
        """Run IsolationForest on each service's RED metric vectors."""
        loop = asyncio.get_event_loop()
        sem = asyncio.Semaphore(4)

        async def _detect(service: str) -> None:
            async with sem:
                metrics = self._feature_store.get_red_metrics(
                    service, window_minutes=self.window_minutes
                )
                if not metrics:
                    return
                try:
                    prediction = await loop.run_in_executor(
                        None, lambda: self._iso_forest.fit_and_predict(service, metrics)
                    )
                except Exception as exc:
                    logger.error(
                        "IsolationForest failed service=%s: %s", service, exc
                    )
                    return
                if prediction is not None:
                    await self._alerter.fire(prediction)

        await asyncio.gather(*[_detect(s) for s in services], return_exceptions=True)

    async def _run_custom_metric_cycle(self) -> None:
        """Forecast all (service, metric) pairs in MetricFeatureStore."""
        store = self._metric_feature_store
        services = store.get_services()
        if not services:
            return

        tasks = []
        for service in services:
            for metric_name in store.get_metric_names(service):
                points = store.get_series(
                    service, metric_name, window_minutes=self.window_minutes
                )
                if len(points) >= self.min_data_points:
                    tasks.append(
                        self._forecast_custom_metric(service, metric_name, points)
                    )

        if not tasks:
            return

        sem = asyncio.Semaphore(8)

        async def _bounded(coro):
            async with sem:
                return await coro

        await asyncio.gather(*[_bounded(t) for t in tasks], return_exceptions=True)

    async def _forecast_custom_metric(
        self, service: str, metric_name: str, points
    ) -> None:
        """Compute and store a forecast for one custom OTel metric."""
        values = np.array([p.value for p in points], dtype=float)
        timestamps = [p.timestamp for p in points]

        loop = asyncio.get_event_loop()
        try:
            model, mse = await loop.run_in_executor(
                None, lambda: select_model(values, self.default_model)
            )
        except Exception as exc:
            logger.error(
                "Custom metric model fit failed service=%s metric=%s: %s",
                service, metric_name, exc,
            )
            return

        last_ts = timestamps[-1]
        if last_ts.tzinfo is None:
            last_ts = last_ts.replace(tzinfo=timezone.utc)

        steps = self.horizon_minutes
        future_ts = [last_ts + timedelta(minutes=i + 1) for i in range(steps)]

        try:
            predicted, lower, upper = model.predict(steps)
        except Exception as exc:
            logger.error(
                "Custom metric prediction failed service=%s metric=%s: %s",
                service, metric_name, exc,
            )
            return

        confidences = np.exp(-np.arange(steps) / steps)
        prediction_points: List[PredictionPoint] = [
            PredictionPoint(
                timestamp=future_ts[i],
                predicted=float(predicted[i]),
                lower_bound=float(lower[i]),
                upper_bound=float(upper[i]),
                confidence=float(confidences[i]),
            )
            for i in range(steps)
        ]

        model_cls = type(model).__name__.lower()
        if "ewma" in model_cls:
            model_type = ModelType.EWMA
        elif "arima" in model_cls:
            model_type = ModelType.ARIMA
        elif "holtwinters" in model_cls:
            model_type = ModelType.HOLTWINTERS
        else:
            model_type = ModelType.EWMA

        anomaly = self._predictor.analyse(
            ForecastResult(
                service_name=service,
                metric_name=metric_name,
                model_used=model_type,
                generated_at=datetime.now(tz=timezone.utc),
                horizon_minutes=self.horizon_minutes,
                predictions=prediction_points,
                mse=float(mse) if mse != float("inf") else None,
            ),
            values,
        )

        result = ForecastResult(
            service_name=service,
            metric_name=metric_name,
            model_used=model_type,
            generated_at=datetime.now(tz=timezone.utc),
            horizon_minutes=self.horizon_minutes,
            predictions=prediction_points,
            mse=float(mse) if mse != float("inf") else None,
            is_anomaly_predicted=anomaly is not None,
            anomaly_severity=anomaly.severity if anomaly else None,
        )
        await self._forecast_store.set(result)

        if anomaly:
            await self._alerter.fire(anomaly)

        logger.debug(
            "Custom metric forecast stored service=%s metric=%s model=%s anomaly=%s",
            service, metric_name, model_type.value, anomaly is not None,
        )

    async def _forecast_service_metric(
        self, service: str, metric: str
    ) -> None:
        """Compute and store a forecast for one (service, metric) pair."""
        points = self._feature_store.get_series(
            service, metric, window_minutes=self.window_minutes
        )
        if len(points) < self.min_data_points:
            logger.debug(
                "Not enough data service=%s metric=%s points=%d",
                service,
                metric,
                len(points),
            )
            return

        values = np.array([p.value for p in points], dtype=float)
        timestamps = [p.timestamp for p in points]

        # P2-A: update hour-of-week baseline with all historical observations
        for pt in points:
            ts = pt.timestamp
            if ts.tzinfo is None:
                from datetime import timezone as _tz
                ts = ts.replace(tzinfo=_tz.utc)
            self._baseline_store.update(service, metric, ts, pt.value)

        # Run in executor to avoid blocking event loop for statsmodels
        loop = asyncio.get_event_loop()
        try:
            model, mse = await loop.run_in_executor(
                None, lambda: select_model(values, self.default_model)
            )
        except Exception as exc:
            logger.error(
                "Model fitting failed service=%s metric=%s: %s",
                service,
                metric,
                exc,
            )
            return

        # Build future timestamps (1-minute steps)
        last_ts = timestamps[-1]
        if last_ts.tzinfo is None:
            last_ts = last_ts.replace(tzinfo=timezone.utc)

        steps = self.horizon_minutes
        future_ts = [
            last_ts + timedelta(minutes=i + 1) for i in range(steps)
        ]

        try:
            predicted, lower, upper = model.predict(steps)
        except Exception as exc:
            logger.error(
                "Prediction failed service=%s metric=%s: %s",
                service,
                metric,
                exc,
            )
            return

        # Compute per-point confidence (decays with horizon)
        confidences = np.exp(-np.arange(steps) / steps)

        prediction_points: List[PredictionPoint] = []
        for i in range(steps):
            prediction_points.append(
                PredictionPoint(
                    timestamp=future_ts[i],
                    predicted=float(predicted[i]),
                    lower_bound=float(lower[i]),
                    upper_bound=float(upper[i]),
                    confidence=float(confidences[i]),
                )
            )

        # Determine model type label
        model_cls = type(model).__name__.lower()
        if "ewma" in model_cls:
            model_type = ModelType.EWMA
        elif "arima" in model_cls:
            model_type = ModelType.ARIMA
        elif "holtwinters" in model_cls:
            model_type = ModelType.HOLTWINTERS
        else:
            model_type = ModelType.EWMA

        # Anomaly detection (P2-A: pass hour-of-week baseline_store)
        anomaly = self._predictor.analyse(
            ForecastResult(
                service_name=service,
                metric_name=metric,
                model_used=model_type,
                generated_at=datetime.now(tz=timezone.utc),
                horizon_minutes=self.horizon_minutes,
                predictions=prediction_points,
                mse=float(mse) if mse != float("inf") else None,
            ),
            values,
            baseline_store=self._baseline_store,
        )

        is_anomaly = anomaly is not None
        severity = anomaly.severity if anomaly else None

        result = ForecastResult(
            service_name=service,
            metric_name=metric,
            model_used=model_type,
            generated_at=datetime.now(tz=timezone.utc),
            horizon_minutes=self.horizon_minutes,
            predictions=prediction_points,
            mse=float(mse) if mse != float("inf") else None,
            is_anomaly_predicted=is_anomaly,
            anomaly_severity=severity,
        )

        await self._forecast_store.set(result)

        if _mse_gauge and result.mse is not None:
            _mse_gauge.labels(
                service=service, metric=metric, model=model_type.value
            ).set(result.mse)

        if is_anomaly and anomaly:
            if _anomaly_counter:
                _anomaly_counter.labels(
                    service=service, severity=anomaly.severity
                ).inc()
            await self._alerter.fire(anomaly)

        logger.debug(
            "Forecast stored service=%s metric=%s model=%s mse=%s anomaly=%s",
            service,
            metric,
            model_type.value,
            f"{mse:.4f}" if mse != float("inf") else "inf",
            is_anomaly,
        )
