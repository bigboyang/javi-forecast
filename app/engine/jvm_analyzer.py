"""JVM health analyzer.

Runs as a background asyncio task alongside the main Forecaster.
Every ``interval_seconds`` it reads from JvmFeatureStore and produces
three types of health signals:

1. OOM Prediction
   - Fits a linear regression on heap_used / heap_max ratio over time.
   - Extrapolates to saturation (ratio = 1.0) to compute time_to_oom.
   - Alerts if time_to_oom < JVM_OOM_WARN_MINUTES.

2. GC Storm Detection
   - Computes Z-score of recent gc_pause_ms_total_delta against historical mean/std.
   - Also checks absolute GC overhead (pause_ms / interval_ms).
   - Alerts if Z-score > threshold or overhead exceeds JVM_GC_OVERHEAD_WARN.

3. Thread Leak Detection
   - Splits the time window into an older and a recent half.
   - Alerts if recent_median / old_median > threshold AND trend is monotonically growing.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

import numpy as np

from ..alerter.webhook import WebhookAlerter
from ..anomaly.predictor import AnomalyPrediction
from ..config import settings
from ..models.jvm import JvmMetricEvent
from .jvm_feature_store import JvmFeatureStore

logger = logging.getLogger(__name__)

# Minimum snapshots before analysis is attempted
_MIN_SNAPSHOTS = 5
# GC storm Z-score threshold
_GC_STORM_Z = 3.0
# Thread growth ratio threshold (recent / historical > threshold → leak candidate)
_THREAD_GROWTH_RATIO = 1.5
# Fraction of diffs that must be positive to declare monotonic growth
_MONOTONIC_THRESHOLD = 0.7


class JvmAnalyzer:
    """Background JVM health analyzer.

    Parameters
    ----------
    jvm_store:
        Source of JVM metric snapshots.
    alerter:
        Fires webhooks when anomalies are detected.
    interval_seconds:
        How often to run the analysis loop.
    oom_warn_minutes:
        Alert if OOM is predicted within this many minutes.
    gc_overhead_warn:
        Alert if GC overhead fraction exceeds this threshold (0.0–1.0).
    """

    def __init__(
        self,
        jvm_store: JvmFeatureStore,
        alerter: WebhookAlerter,
        interval_seconds: int = 60,
        oom_warn_minutes: float = settings.JVM_OOM_WARN_MINUTES,
        gc_overhead_warn: float = settings.JVM_GC_OVERHEAD_WARN,
    ) -> None:
        self._store = jvm_store
        self._alerter = alerter
        self.interval_seconds = interval_seconds
        self.oom_warn_minutes = oom_warn_minutes
        self.gc_overhead_warn = gc_overhead_warn

        self._task: asyncio.Task | None = None
        self._running = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="jvm-analyzer")
        logger.info(
            "JvmAnalyzer started interval=%ds oom_warn=%dm gc_overhead_warn=%.0f%%",
            self.interval_seconds,
            int(self.oom_warn_minutes),
            self.gc_overhead_warn * 100,
        )

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("JvmAnalyzer stopped")

    # ------------------------------------------------------------------
    # Internal loop
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        while self._running:
            try:
                await self._run_cycle()
            except Exception as exc:
                logger.error("JvmAnalyzer cycle error: %s", exc, exc_info=True)
            await asyncio.sleep(self.interval_seconds)

    async def _run_cycle(self) -> None:
        services = self._store.get_services()
        if not services:
            return
        logger.debug("JvmAnalyzer cycle: services=%d", len(services))
        for service in services:
            snapshots = self._store.get_snapshots(service, window_minutes=60)
            if len(snapshots) < _MIN_SNAPSHOTS:
                continue
            await self._analyse_service(service, snapshots)

    async def _analyse_service(
        self, service: str, snapshots: list[JvmMetricEvent]
    ) -> None:
        loop = asyncio.get_event_loop()
        alerts: list[AnomalyPrediction] = await loop.run_in_executor(
            None, lambda: self._compute_signals(service, snapshots)
        )
        for alert in alerts:
            await self._alerter.fire(alert)

    # ------------------------------------------------------------------
    # Signal computation (runs in executor, no async)
    # ------------------------------------------------------------------

    def _compute_signals(
        self, service: str, snapshots: list[JvmMetricEvent]
    ) -> list[AnomalyPrediction]:
        alerts: list[AnomalyPrediction] = []
        for fn in (self._check_oom, self._check_gc_storm, self._check_thread_leak):
            result = fn(service, snapshots)
            if result is not None:
                alerts.append(result)
        return alerts

    # ------------------------------------------------------------------
    # OOM prediction via linear regression
    # ------------------------------------------------------------------

    def _check_oom(
        self, service: str, snapshots: list[JvmMetricEvent]
    ) -> AnomalyPrediction | None:
        """Predict time to OOM via linear regression on heap usage ratio."""
        ratios: list[float] = []
        timestamps: list[float] = []  # seconds since first snapshot

        t0 = snapshots[0].timestamp_nano / 1e9
        for s in snapshots:
            if s.heap_max_bytes > 0:
                ratios.append(s.heap_used_bytes / s.heap_max_bytes)
                timestamps.append(s.timestamp_nano / 1e9 - t0)

        if len(ratios) < _MIN_SNAPSHOTS:
            return None

        x = np.array(timestamps, dtype=float)
        y = np.array(ratios, dtype=float)

        coeffs = np.polyfit(x, y, 1)
        slope = float(coeffs[0])

        # Only alert on increasing heap trends
        if slope <= 0:
            return None

        current_ratio = float(y[-1])
        time_to_oom_seconds = (1.0 - current_ratio) / slope
        time_to_oom_minutes = time_to_oom_seconds / 60.0

        if time_to_oom_minutes > self.oom_warn_minutes or time_to_oom_seconds < 0:
            return None

        severity = (
            "critical"
            if time_to_oom_minutes < (self.oom_warn_minutes / 2)
            else "warn"
        )
        now = datetime.now(tz=UTC)
        predicted_at = now + timedelta(seconds=max(0.0, time_to_oom_seconds))

        logger.warning(
            "OOM predicted service=%s current_heap=%.1f%% time_to_oom=%.1fm severity=%s",
            service,
            current_ratio * 100,
            time_to_oom_minutes,
            severity,
        )
        return AnomalyPrediction(
            service_name=service,
            metric_name="jvm_heap_oom",
            severity=severity,
            predicted_value=current_ratio,
            expected_value=float(np.mean(y)),
            z_score=abs(current_ratio - float(np.mean(y))) / max(float(np.std(y)), 1e-6),
            predicted_at=predicted_at,
            seconds_until_anomaly=max(0.0, time_to_oom_seconds),
        )

    # ------------------------------------------------------------------
    # GC storm detection
    # ------------------------------------------------------------------

    def _check_gc_storm(
        self, service: str, snapshots: list[JvmMetricEvent]
    ) -> AnomalyPrediction | None:
        """Detect GC storm via Z-score on gc_pause_ms_total_delta."""
        pauses = np.array(
            [s.gc_pause_ms_total_delta for s in snapshots], dtype=float
        )
        mean = float(np.mean(pauses))
        std = float(np.std(pauses))
        if std < 1e-6:
            return None

        current = float(pauses[-1])
        z = (current - mean) / std

        # 60 s interval → overhead fraction = pause_ms / 60_000
        gc_overhead = current / 60_000.0
        overhead_exceeded = gc_overhead > self.gc_overhead_warn

        if abs(z) < _GC_STORM_Z and not overhead_exceeded:
            return None

        severity = (
            "critical"
            if abs(z) >= (_GC_STORM_Z + 1) or gc_overhead > (self.gc_overhead_warn * 2)
            else "warn"
        )
        now = datetime.now(tz=UTC)

        logger.warning(
            "GC storm detected service=%s gc_pause_ms=%.1f mean=%.1f z=%.2f "
            "overhead=%.1f%% severity=%s",
            service,
            current,
            mean,
            z,
            gc_overhead * 100,
            severity,
        )
        return AnomalyPrediction(
            service_name=service,
            metric_name="jvm_gc_storm",
            severity=severity,
            predicted_value=current,
            expected_value=mean,
            z_score=abs(z),
            predicted_at=now,
            seconds_until_anomaly=0.0,
        )

    # ------------------------------------------------------------------
    # Thread leak detection
    # ------------------------------------------------------------------

    def _check_thread_leak(
        self, service: str, snapshots: list[JvmMetricEvent]
    ) -> AnomalyPrediction | None:
        """Detect thread leak via monotonic growth and median ratio."""
        counts = np.array([s.thread_count for s in snapshots], dtype=float)
        if len(counts) < _MIN_SNAPSHOTS * 2:
            return None

        half = len(counts) // 2
        old_median = float(np.median(counts[:half]))
        recent_median = float(np.median(counts[half:]))

        if old_median < 1:
            return None

        ratio = recent_median / old_median
        recent = counts[half:]
        diffs = np.diff(recent)
        monotonic_fraction = (
            float(np.sum(diffs > 0)) / len(diffs) if len(diffs) > 0 else 0.0
        )

        if ratio < _THREAD_GROWTH_RATIO or monotonic_fraction < _MONOTONIC_THRESHOLD:
            return None

        severity = (
            "critical" if ratio > (_THREAD_GROWTH_RATIO * 1.5) else "warn"
        )
        now = datetime.now(tz=UTC)

        logger.warning(
            "Thread leak suspected service=%s old_median=%.0f recent_median=%.0f "
            "ratio=%.2f monotonic=%.0f%% severity=%s",
            service,
            old_median,
            recent_median,
            ratio,
            monotonic_fraction * 100,
            severity,
        )
        return AnomalyPrediction(
            service_name=service,
            metric_name="jvm_thread_leak",
            severity=severity,
            predicted_value=recent_median,
            expected_value=old_median,
            z_score=ratio,
            predicted_at=now,
            seconds_until_anomaly=0.0,
        )
