"""Granger causality analyzer.

Periodically tests whether each service's error_rate Granger-causes every
other service's error_rate, building a directed causal graph stored in
DependencyMap.

Uses statsmodels.tsa.stattools.grangercausalitytests.

Test format: for each (source → target) pair, construct a 2-column matrix
  [target_series, source_series]
then call grangercausalitytests, which tests whether *source* (column 1)
Granger-causes *target* (column 0).

The best p-value across all tested lags (1 … maxlag) is taken from the
ssr_chi2test result for each lag.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np

from ..config import settings
from .dependency_map import DependencyMap
from .feature_store import FeatureStore

logger = logging.getLogger(__name__)

# Minimum aligned observations needed for a reliable Granger test
_MIN_POINTS = 30

# Default maximum lag to test
_DEFAULT_MAXLAG = 5


class GrangerAnalyzer:
    """Background Granger causality analyzer.

    Every ``interval_seconds``, tests all ordered service pairs (A → B)
    and updates DependencyMap with statistically significant causal
    relationships.

    Parameters
    ----------
    feature_store:
        Source of error_rate time-series.
    dependency_map:
        Graph updated with Granger test results.
    interval_seconds:
        How often to run the analysis.
    p_threshold:
        Maximum p-value for an edge to be considered significant.
    maxlag:
        Maximum lag order to test per pair.
    """

    def __init__(
        self,
        feature_store: FeatureStore,
        dependency_map: DependencyMap,
        interval_seconds: int = settings.GRANGER_INTERVAL_SECONDS,
        p_threshold: float = settings.GRANGER_P_THRESHOLD,
        maxlag: int = _DEFAULT_MAXLAG,
    ) -> None:
        self._store = feature_store
        self._dep_map = dependency_map
        self.interval_seconds = interval_seconds
        self.p_threshold = p_threshold
        self.maxlag = maxlag

        self._task: Optional[asyncio.Task] = None
        self._running = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="granger-analyzer")
        logger.info(
            "GrangerAnalyzer started interval=%ds p_threshold=%.2f maxlag=%d",
            self.interval_seconds,
            self.p_threshold,
            self.maxlag,
        )

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("GrangerAnalyzer stopped")

    # ------------------------------------------------------------------
    # Internal loop
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        while self._running:
            try:
                await self._run_cycle()
            except Exception as exc:
                logger.error("GrangerAnalyzer cycle error: %s", exc, exc_info=True)
            await asyncio.sleep(self.interval_seconds)

    async def _run_cycle(self) -> None:
        services = self._store.get_services()
        if len(services) < 2:
            return

        # Collect aligned error_rate series (last 60 minutes)
        service_data: Dict[str, List[Tuple[datetime, float]]] = {}
        for service in services:
            pts = self._store.get_series(service, "error_rate", window_minutes=60)
            if len(pts) >= _MIN_POINTS:
                service_data[service] = [(p.timestamp, p.value) for p in pts]

        if len(service_data) < 2:
            return

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None, lambda: self._test_all_pairs(service_data)
        )

    # ------------------------------------------------------------------
    # Synchronous Granger tests (runs in executor)
    # ------------------------------------------------------------------

    def _test_all_pairs(
        self, service_data: Dict[str, List[Tuple[datetime, float]]]
    ) -> None:
        try:
            from statsmodels.tsa.stattools import grangercausalitytests
        except ImportError:
            logger.warning("statsmodels not available – GrangerAnalyzer disabled")
            return

        names = list(service_data.keys())

        # Align all series to common timestamps (inner join)
        ts_sets = [set(ts for ts, _ in service_data[n]) for n in names]
        common_ts = sorted(ts_sets[0].intersection(*ts_sets[1:]))

        if len(common_ts) < _MIN_POINTS:
            return

        aligned: Dict[str, np.ndarray] = {}
        for name in names:
            lookup = {ts: v for ts, v in service_data[name]}
            aligned[name] = np.array([lookup[ts] for ts in common_ts], dtype=float)

        for source in names:
            for target in names:
                if source == target:
                    continue
                self._test_pair(
                    source, target,
                    aligned[source], aligned[target],
                    grangercausalitytests,
                )

    def _test_pair(
        self,
        source: str,
        target: str,
        source_series: np.ndarray,
        target_series: np.ndarray,
        grangercausalitytests,
    ) -> None:
        """Test whether *source* Granger-causes *target*.

        grangercausalitytests expects [y, x] where it tests x→y.
        We pass [target, source] to test source→target.
        """
        data = np.column_stack([target_series, source_series])
        try:
            results = grangercausalitytests(data, maxlag=self.maxlag, verbose=False)
        except Exception as exc:
            logger.debug("Granger test failed %s→%s: %s", source, target, exc)
            return

        # Find minimum p-value across all lags (1..maxlag)
        best_p = 1.0
        best_lag = 1
        for lag in range(1, self.maxlag + 1):
            lag_result = results.get(lag)
            if lag_result is None:
                continue
            test_stats = lag_result[0]  # dict of test name → (stat, p, df, ...)
            chi2_result = test_stats.get("ssr_chi2test")
            if chi2_result is not None and chi2_result[1] < best_p:
                best_p = chi2_result[1]
                best_lag = lag

        self._dep_map.update(source, target, best_p, best_lag)

        if best_p <= self.p_threshold:
            logger.info(
                "Granger causality detected: %s → %s  p=%.4f lag=%d",
                source, target, best_p, best_lag,
            )
