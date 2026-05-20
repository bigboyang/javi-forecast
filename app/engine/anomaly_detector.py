"""ClickHouse-based anomaly detector (ported from javi-collector Go anomaly pkg).

Detection pipeline (every ANOMALY_INTERVAL_SECONDS):
  1. Query mv_red_1m_state for the last completed minute.
  2. Query red_baseline (FINAL) for all (service, span, route, dow, hour) baselines.
  3. Z-score analysis: latency_p95_spike / error_rate_spike / traffic_drop.
  4. IsolationForest multivariate score: multivariate_anomaly.
  5. Batch-INSERT detected anomalies into ClickHouse apm.anomalies.
  6. Fire webhook alerts for new anomalies.

IsolationForest retraining (every ANOMALY_TRAIN_INTERVAL_HOURS):
  - Load entire red_baseline table as feature matrix.
  - Fit with normalisation anchored to max(p95_ms) / max(avg_rps).
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional

import numpy as np

from ..config import settings
from .prom_metrics import anomalies_detected, anomalies_suppressed, alerts_fired, alerts_active

logger = logging.getLogger(__name__)

_SUPPRESSION_WINDOW = timedelta(minutes=settings.ANOMALY_SUPPRESSION_MINUTES)


class _IForestState:
    __slots__ = ("clf", "max_p95", "max_rps", "trained_at")

    def __init__(self, clf, max_p95: float, max_rps: float) -> None:
        self.clf = clf
        self.max_p95 = max_p95
        self.max_rps = max_rps
        self.trained_at: datetime = datetime.now(tz=timezone.utc)


class AnomalyDetector:
    """Background detector that mirrors the Go anomaly.Detector.

    Parameters
    ----------
    clickhouse:
        Connected ClickHouseStore instance.
    alerter:
        WebhookAlerter to fire when anomalies are detected.
    """

    def __init__(self, clickhouse, alerter=None, alert_store=None, anomaly_clusterer=None) -> None:
        self._ch = clickhouse
        self._alerter = alerter
        self._alert_store = alert_store
        self._clusterer = anomaly_clusterer
        self._iforest: Optional[_IForestState] = None
        self._suppressed: dict[str, datetime] = {}  # key → suppress_until
        self._task: Optional[asyncio.Task] = None
        self._last_trained: Optional[datetime] = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="anomaly-detector")
        logger.info(
            "AnomalyDetector started interval=%ds train_interval=%dh",
            settings.ANOMALY_INTERVAL_SECONDS,
            settings.ANOMALY_TRAIN_INTERVAL_HOURS,
        )

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("AnomalyDetector stopped")

    # ------------------------------------------------------------------
    # Background loop
    # ------------------------------------------------------------------

    async def _run(self) -> None:
        # Train IForest immediately on startup
        await self._train_forest()
        while True:
            await asyncio.sleep(settings.ANOMALY_INTERVAL_SECONDS)
            try:
                train_interval = timedelta(hours=settings.ANOMALY_TRAIN_INTERVAL_HOURS)
                if (
                    self._last_trained is None
                    or (datetime.now(tz=timezone.utc) - self._last_trained) >= train_interval
                ):
                    await self._train_forest()
                await self._detect()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("anomaly detect cycle error: %s", exc)

    # ------------------------------------------------------------------
    # IsolationForest training
    # ------------------------------------------------------------------

    async def _train_forest(self) -> None:
        try:
            baselines = await self._ch.query_baseline_all()
        except Exception as exc:
            logger.error("anomaly: baseline load failed: %s", exc)
            return

        if len(baselines) < 10:
            logger.warning("anomaly: insufficient baseline rows for IForest n=%d", len(baselines))
            return

        max_p95 = max((b["p95_ms"] for b in baselines), default=1.0) or 1.0
        max_rps = max((b["avg_rps"] for b in baselines), default=1.0) or 1.0

        matrix = np.array(
            [
                [
                    b["p50_ms"] / max_p95,
                    b["p95_ms"] / max_p95,
                    b["p99_ms"] / max_p95,
                    b["error_rate"],
                    b["avg_rps"] / max_rps,
                ]
                for b in baselines
            ],
            dtype=float,
        )

        try:
            from sklearn.ensemble import IsolationForest
        except ImportError:
            logger.warning("scikit-learn not available – IForest disabled")
            return

        loop = asyncio.get_running_loop()
        clf = await loop.run_in_executor(
            None,
            lambda: IsolationForest(
                n_estimators=100, contamination=0.05, random_state=42, n_jobs=1
            ).fit(matrix),
        )
        self._iforest = _IForestState(clf, max_p95, max_rps)
        self._last_trained = datetime.now(tz=timezone.utc)
        logger.info(
            "anomaly: IsolationForest trained samples=%d max_p95=%.2f max_rps=%.2f",
            len(baselines), max_p95, max_rps,
        )

    # ------------------------------------------------------------------
    # Detection cycle
    # ------------------------------------------------------------------

    async def _detect(self) -> None:
        try:
            points = await self._ch.query_current_red()
        except Exception as exc:
            logger.error("anomaly: RED query failed: %s", exc)
            return

        if not points:
            return

        try:
            baselines = await self._ch.query_baseline_all()
        except Exception as exc:
            logger.error("anomaly: baseline query failed: %s", exc)
            return

        # Build lookup: (svc, span, route, dow, hour) → baseline row
        baseline_map: dict[tuple, dict] = {}
        for b in baselines:
            key = (b["service_name"], b["span_name"], b["http_route"],
                   b["day_of_week"], b["hour_of_day"])
            baseline_map[key] = b

        detected: list[dict] = []
        z_warn = settings.ANOMALY_Z_WARN
        z_crit = settings.ANOMALY_Z_CRITICAL
        if_thresh = settings.ANOMALY_IF_THRESHOLD

        max_p95 = self._iforest.max_p95 if self._iforest else 1.0
        max_rps = self._iforest.max_rps if self._iforest else 1.0

        for pt in points:
            dt = pt["minute"]
            dow = dt.isoweekday()  # Mon=1 … Sun=7
            hour = dt.hour

            b = baseline_map.get(
                (pt["service_name"], pt["span_name"], pt["http_route"], dow, hour)
            )
            if b is None:
                continue

            total = pt["req_count"]
            rps = total / 60.0

            # ── Z-score: Latency P95 ──────────────────────────────────────
            if b["p95_ms"] > 0:
                std_est = max(
                    (b["p99_ms"] - b["p50_ms"]) / 2.326,
                    b["p95_ms"] * 0.05,
                )
                std_est = std_est or 1e-6
                z = (pt["p95_ms"] - b["p95_ms"]) / std_est
                if z >= z_warn:
                    sev = "critical" if z >= z_crit else "warning"
                    detected.append(self._make_record(
                        pt, "latency_p95_spike", pt["p95_ms"], b["p95_ms"], z, sev
                    ))

            # ── Z-score: Error Rate ───────────────────────────────────────
            if b["sample_count"] >= 10:
                p = b["error_rate"]
                n = b["sample_count"]
                base_std = math.sqrt(p * (1 - p) / n) if n > 0 else 0.001
                base_std = max(base_std, 0.001)
                z = (pt["error_rate"] - b["error_rate"]) / base_std
                if z >= z_warn:
                    sev = "critical" if z >= z_crit else "warning"
                    detected.append(self._make_record(
                        pt, "error_rate_spike", pt["error_rate"], b["error_rate"], z, sev
                    ))

            # ── Traffic Drop ──────────────────────────────────────────────
            if b["avg_rps"] > 0 and rps < b["avg_rps"] * 0.5:
                ratio = rps / b["avg_rps"]
                z_proxy = (1 - ratio) * 10
                detected.append(self._make_record(
                    pt, "traffic_drop", rps, b["avg_rps"], z_proxy, "critical"
                ))

            # ── IsolationForest ───────────────────────────────────────────
            if self._iforest is not None:
                feat = np.array([[
                    pt["p50_ms"] / max_p95,
                    pt["p95_ms"] / max_p95,
                    pt["p99_ms"] / max_p95,
                    pt["error_rate"],
                    rps / max_rps,
                ]])
                score = float(-self._iforest.clf.score_samples(feat)[0])
                if score >= if_thresh:
                    sev = "critical" if score >= 0.75 else "warning"
                    detected.append(self._make_record(
                        pt, "multivariate_anomaly", score, if_thresh, score, sev
                    ))

        if not detected:
            return

        # Suppression window filter
        now = datetime.now(tz=timezone.utc)
        to_insert: list[dict] = []
        for a in detected:
            key = f"{a['service_name']}\x00{a['span_name']}\x00{a['anomaly_type']}"
            suppress_until = self._suppressed.get(key)
            if suppress_until and now < suppress_until:
                continue
            self._suppressed[key] = now + _SUPPRESSION_WINDOW
            to_insert.append(a)

        # Purge expired suppression entries
        expired = [k for k, v in self._suppressed.items() if now > v]
        for k in expired:
            del self._suppressed[k]

        if not to_insert:
            return

        suppressed_count = len(detected) - len(to_insert)
        if suppressed_count:
            anomalies_suppressed.inc(suppressed_count)

        try:
            await self._ch.insert_anomalies(to_insert)
            logger.info(
                "anomaly: recorded=%d suppressed=%d",
                len(to_insert), suppressed_count,
            )
        except Exception as exc:
            logger.error("anomaly: insert failed: %s", exc)
            return

        # Prometheus counters + alert lifecycle + clustering
        for a in to_insert:
            anomalies_detected.labels(
                anomaly_type=a["anomaly_type"], severity=a["severity"]
            ).inc()

        if self._alerter:
            for a in to_insert:
                await self._alerter.fire_anomaly(a)
                alerts_fired.labels(severity=a["severity"]).inc()

        if self._alert_store is not None:
            for a in to_insert:
                await self._alert_store.create_alert(a)
            alerts_active.set(self._alert_store.count_firing())

        if self._clusterer is not None:
            for a in to_insert:
                await self._clusterer.ingest(a)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _make_record(
        pt: dict,
        anomaly_type: str,
        current: float,
        baseline: float,
        z: float,
        severity: str,
    ) -> dict:
        return {
            "id": uuid.uuid4().hex,
            "service_name": pt["service_name"],
            "span_name": pt["span_name"],
            "anomaly_type": anomaly_type,
            "minute": pt["minute"],
            "current_value": current,
            "baseline_value": baseline,
            "z_score": z,
            "severity": severity,
        }
