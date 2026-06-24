"""Cross-service VAR (Vector Autoregression) forecaster.

Fits a statsmodels VAR model over the aligned error_rate time-series of
all known services, capturing cross-service temporal correlations that the
per-service EWMA/ARIMA models miss.

Background loop (every FORECAST_INTERVAL_SECONDS):
  1. Collect per-service error_rate series (window_minutes lookback).
  2. Drop flat series (std < 1e-4) to avoid singular covariance.
  3. Inner-join the remaining series onto a common 1-minute timestamp grid.
  4. Fit VAR(p) via AIC (maxlags capped at min(5, T//4)).
  5. Verify stability (reject if not is_stable()).
  6. Generate horizon_minutes-step-ahead forecasts with 95 % confidence bands.
  7. Check for NaN in forecast; discard if found.
  8. Store one ForecastResult(metric_name='error_rate_var') per service.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

import numpy as np

from ..config import settings
from ..models.forecast import ForecastResult, ModelType, PredictionPoint
from ..store.forecast_store import ForecastStore
from .feature_store import FeatureStore

logger = logging.getLogger(__name__)

# Minimum aligned time-steps before attempting VAR fitting
_MIN_ALIGNED_POINTS = 30

# Maximum VAR lag order tested when selecting via AIC
_MAX_LAG = 5

# Series with std below this are considered flat and excluded
_FLAT_STD_THRESHOLD = 1e-4


def _collect_and_align(
    service_series: dict[str, list[tuple[datetime, float]]]
) -> tuple[list[str], np.ndarray]:
    """Align per-service time-series to a common timestamp grid.

    1. Drop services with flat (near-zero variance) error_rate.
    2. Inner-join on minute timestamps.
    3. Return (names, matrix) where matrix.shape == (T, n_services).
    """
    # Drop flat series
    filtered: dict[str, list[tuple[datetime, float]]] = {}
    for name, pts in service_series.items():
        values = np.array([v for _, v in pts], dtype=float)
        if values.std() >= _FLAT_STD_THRESHOLD:
            filtered[name] = pts

    if len(filtered) < 2:
        return [], np.empty((0, 0))

    # Inner join on timestamps
    ts_sets = [set(ts for ts, _ in v) for v in filtered.values()]
    common_ts = sorted(ts_sets[0].intersection(*ts_sets[1:]))

    if len(common_ts) < _MIN_ALIGNED_POINTS:
        return [], np.empty((0, 0))

    names = list(filtered.keys())
    matrix = np.zeros((len(common_ts), len(names)), dtype=float)
    ts_idx = {ts: i for i, ts in enumerate(common_ts)}
    for col, name in enumerate(names):
        lookup = {ts: v for ts, v in filtered[name]}
        for ts, row in ts_idx.items():
            matrix[row, col] = lookup.get(ts, np.nan)

    # Drop rows with any NaN
    valid = ~np.any(np.isnan(matrix), axis=1)
    return names, matrix[valid]


class VarForecaster:
    """Background VAR cross-service forecaster.

    Parameters
    ----------
    feature_store:
        Source of RED metric time-series.
    forecast_store:
        Destination for computed ForecastResult objects.
    interval_seconds:
        How often to run the VAR forecast cycle.
    horizon_minutes:
        Number of steps ahead to forecast.
    window_minutes:
        Lookback window for historical data.
    min_services:
        Minimum non-flat services required to run VAR.
    """

    def __init__(
        self,
        feature_store: FeatureStore,
        forecast_store: ForecastStore,
        interval_seconds: int = settings.FORECAST_INTERVAL_SECONDS,
        horizon_minutes: int = settings.FORECAST_HORIZON_MINUTES,
        window_minutes: int = settings.FEATURE_WINDOW_MINUTES,
        min_services: int = 2,
    ) -> None:
        self._feature_store = feature_store
        self._forecast_store = forecast_store
        self.interval_seconds = interval_seconds
        self.horizon_minutes = horizon_minutes
        self.window_minutes = window_minutes
        self.min_services = min_services

        self._task: asyncio.Task | None = None
        self._running = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="var-forecaster")
        logger.info(
            "VarForecaster started interval=%ds horizon=%dm",
            self.interval_seconds,
            self.horizon_minutes,
        )

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("VarForecaster stopped")

    # ------------------------------------------------------------------
    # Internal loop
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        while self._running:
            try:
                await self._run_cycle()
            except Exception as exc:
                logger.error("VarForecaster cycle error: %s", exc, exc_info=True)
            await asyncio.sleep(self.interval_seconds)

    async def _run_cycle(self) -> None:
        services = self._feature_store.get_services()
        if len(services) < self.min_services:
            logger.debug(
                "VarForecaster: need >= %d services, have %d – skipping",
                self.min_services,
                len(services),
            )
            return

        service_series: dict[str, list[tuple[datetime, float]]] = {}
        for service in services:
            pts = self._feature_store.get_series(
                service, "error_rate", window_minutes=self.window_minutes
            )
            if pts:
                service_series[service] = [(p.timestamp, p.value) for p in pts]

        if len(service_series) < self.min_services:
            return

        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(
            None, lambda: self._fit_and_forecast(service_series)
        )
        for result in results:
            await self._forecast_store.set(result)

        if results:
            logger.info("VarForecaster: stored %d service forecasts", len(results))

    # ------------------------------------------------------------------
    # Synchronous VAR fitting (runs inside thread-pool executor)
    # ------------------------------------------------------------------

    def _fit_and_forecast(
        self,
        service_series: dict[str, list[tuple[datetime, float]]],
    ) -> list[ForecastResult]:
        try:
            from statsmodels.tsa.vector_ar.var_model import VAR
        except ImportError:
            logger.warning("statsmodels not available – VarForecaster disabled")
            return []

        names, matrix = _collect_and_align(service_series)
        if not names or matrix.shape[0] < _MIN_ALIGNED_POINTS:
            return []

        T = matrix.shape[0]
        max_lag = max(1, min(_MAX_LAG, T // 4))

        try:
            fitted = VAR(matrix).fit(maxlags=max_lag, ic="aic", verbose=False)
        except Exception as exc:
            logger.error("VAR fit failed: %s", exc)
            return []

        # Discard unstable models (explosive eigenvalues)
        if not fitted.is_stable():
            logger.warning("VarForecaster: fitted model is not stable – discarding")
            return []

        lag_order = fitted.k_ar
        if lag_order == 0:
            return []

        forecast_input = matrix[-lag_order:]
        try:
            lower, mid, upper = fitted.forecast_interval(
                forecast_input, steps=self.horizon_minutes, alpha=0.05
            )
        except Exception as exc:
            logger.error("VAR forecast_interval failed: %s", exc)
            return []

        # Discard if any NaN in forecast
        if np.isnan(mid).any() or np.isnan(lower).any() or np.isnan(upper).any():
            logger.warning("VarForecaster: NaN in forecast – discarding")
            return []

        # Clamp lower bounds to 0 (error_rate ≥ 0)
        lower = np.maximum(lower, 0.0)
        mid = np.maximum(mid, 0.0)

        now = datetime.now(tz=UTC)
        confidences = np.exp(-np.arange(self.horizon_minutes) / self.horizon_minutes)
        future_ts = [now + timedelta(minutes=i + 1) for i in range(self.horizon_minutes)]

        # In-sample MSE per service
        try:
            in_sample = fitted.fittedvalues  # shape (T - lag_order, n_series)
            actual = matrix[lag_order:]
            mse_per_col = np.mean((in_sample - actual) ** 2, axis=0)
        except Exception:
            mse_per_col = np.full(len(names), np.nan)

        results: list[ForecastResult] = []
        for col, service in enumerate(names):
            points = [
                PredictionPoint(
                    timestamp=future_ts[i],
                    predicted=float(mid[i, col]),
                    lower_bound=float(lower[i, col]),
                    upper_bound=float(upper[i, col]),
                    confidence=float(confidences[i]),
                )
                for i in range(self.horizon_minutes)
            ]
            mse = float(mse_per_col[col]) if not np.isnan(mse_per_col[col]) else None
            results.append(
                ForecastResult(
                    service_name=service,
                    metric_name="error_rate_var",
                    model_used=ModelType.VAR,
                    generated_at=now,
                    horizon_minutes=self.horizon_minutes,
                    predictions=points,
                    mse=mse,
                )
            )

        return results
