"""SLO burn rate analyzer.

Implements the multi-window burn rate alerting strategy from Google SRE
(Chapter 5 of the SRE Workbook).

Two alerting tiers triggered when BOTH windows exceed their threshold:
  - Critical / page:   fast_burn_rate > 14.4  AND  slow_burn_rate > 14.4
  - Warning  / ticket: fast_burn_rate >  6.0  AND  slow_burn_rate >  6.0

burn_rate = observed_error_rate / slo_error_budget
slo_error_budget = 1.0 - slo_target  (e.g. 99.9 % target → budget = 0.001)

A burn_rate of 1.0 means the service is consuming its monthly error budget
at exactly the expected rate.  14.4 means it would exhaust the budget in
2 hours (1/720 of the month × 14.4 × 720 h = 1 month).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from ..alerter.webhook import WebhookAlerter
from ..anomaly.predictor import AnomalyPrediction
from ..config import settings
from .feature_store import FeatureStore

logger = logging.getLogger(__name__)

# Google SRE multi-window thresholds
_PAGE_BURN_RATE = 14.4
_TICKET_BURN_RATE = 6.0

# Window lengths (minutes)
_FAST_WINDOW_MINUTES = 5
_SLOW_WINDOW_MINUTES = 60


class BurnRateAnalyzer:
    """Background SLO burn rate analyzer.

    Parameters
    ----------
    feature_store:
        Source of RED metric time series.
    alerter:
        Fires webhooks when burn rate thresholds are exceeded.
    slo_target:
        Desired service availability (e.g. 0.999 for 99.9 %).
    page_burn_rate:
        Burn rate threshold for a *critical* (page-worthy) alert.
    ticket_burn_rate:
        Burn rate threshold for a *warn* (ticket-worthy) alert.
    interval_seconds:
        How often to run the analysis loop.
    """

    def __init__(
        self,
        feature_store: FeatureStore,
        alerter: WebhookAlerter,
        slo_target: float = settings.SLO_TARGET,
        page_burn_rate: float = _PAGE_BURN_RATE,
        ticket_burn_rate: float = _TICKET_BURN_RATE,
        interval_seconds: int = 60,
    ) -> None:
        self._store = feature_store
        self._alerter = alerter
        self.slo_error_budget = 1.0 - slo_target
        self.page_burn_rate = page_burn_rate
        self.ticket_burn_rate = ticket_burn_rate
        self.interval_seconds = interval_seconds

        self._task: Optional[asyncio.Task] = None
        self._running = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="burn-rate-analyzer")
        logger.info(
            "BurnRateAnalyzer started slo_budget=%.4f page_threshold=%.1f ticket_threshold=%.1f",
            self.slo_error_budget,
            self.page_burn_rate,
            self.ticket_burn_rate,
        )

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("BurnRateAnalyzer stopped")

    # ------------------------------------------------------------------
    # Internal loop
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        while self._running:
            try:
                await self._run_cycle()
            except Exception as exc:
                logger.error("BurnRateAnalyzer cycle error: %s", exc, exc_info=True)
            await asyncio.sleep(self.interval_seconds)

    async def _run_cycle(self) -> None:
        services = self._store.get_services()
        if not services:
            return
        logger.debug("BurnRateAnalyzer cycle: services=%d", len(services))
        for service in services:
            await self._analyse_service(service)

    async def _analyse_service(self, service: str) -> None:
        loop = asyncio.get_event_loop()
        prediction = await loop.run_in_executor(
            None, lambda: self._compute_burn_rate(service)
        )
        if prediction is not None:
            await self._alerter.fire(prediction)

    # ------------------------------------------------------------------
    # Burn rate computation (sync, runs in executor)
    # ------------------------------------------------------------------

    def _compute_burn_rate(self, service: str) -> Optional[AnomalyPrediction]:
        fast_pts = self._store.get_series(
            service, "error_rate", window_minutes=_FAST_WINDOW_MINUTES
        )
        slow_pts = self._store.get_series(
            service, "error_rate", window_minutes=_SLOW_WINDOW_MINUTES
        )

        # Need at least 2 points in each window
        if len(fast_pts) < 2 or len(slow_pts) < 2:
            return None

        fast_values = [p.value for p in fast_pts]
        slow_values = [p.value for p in slow_pts]

        fast_mean = sum(fast_values) / len(fast_values)
        slow_mean = sum(slow_values) / len(slow_values)

        # Avoid division by zero for zero-budget edge case
        if self.slo_error_budget <= 0:
            return None

        fast_burn = fast_mean / self.slo_error_budget
        slow_burn = slow_mean / self.slo_error_budget

        severity: Optional[str] = None
        if fast_burn >= self.page_burn_rate and slow_burn >= self.page_burn_rate:
            severity = "critical"
        elif fast_burn >= self.ticket_burn_rate and slow_burn >= self.ticket_burn_rate:
            severity = "warn"

        if severity is None:
            return None

        now = datetime.now(tz=timezone.utc)
        logger.warning(
            "SLO burn rate alert service=%s fast_burn=%.2f slow_burn=%.2f "
            "fast_error=%.4f slow_error=%.4f severity=%s",
            service,
            fast_burn,
            slow_burn,
            fast_mean,
            slow_mean,
            severity,
        )

        return AnomalyPrediction(
            service_name=service,
            metric_name="slo_burn_rate",
            severity=severity,
            predicted_value=fast_burn,
            expected_value=1.0,          # baseline burn rate = 1.0 (on-budget)
            z_score=fast_burn,           # burn rate itself acts as the "score"
            predicted_at=now,
            seconds_until_anomaly=0.0,
        )
