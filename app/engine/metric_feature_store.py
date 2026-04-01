"""In-memory ring-buffer store for general OTel metric time series.

Stores MetricEvent data-points published by javi-collector's MetricProducer.
Each (service_name, metric_name) pair gets an independent deque capped at
``maxlen`` entries.  All mutations are protected by a per-key asyncio.Lock.

Key differences from FeatureStore (which accumulates raw spans into RED buckets):
- MetricFeatureStore accepts **pre-computed** metric values from OTLP
  (gauge/sum/histogram averages already resolved by the collector).
- No bucketing / aggregation; each data-point is stored as-is.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Deque, Dict, List, Optional, Tuple

from ..models.metric import MetricEvent, MetricPoint

logger = logging.getLogger(__name__)

_DEFAULT_MAXLEN = 4320   # 72 h at 1-min cadence


class MetricFeatureStore:
    """Thread-safe ring-buffer keyed by (service_name, metric_name).

    Parameters
    ----------
    maxlen:
        Maximum number of data-points kept per (service, metric) pair.
    """

    def __init__(self, maxlen: int = _DEFAULT_MAXLEN) -> None:
        self.maxlen = maxlen
        # (service_name, metric_name) → deque[(datetime, float)]
        self._series: Dict[Tuple[str, str], Deque[Tuple[datetime, float]]] = defaultdict(
            lambda: deque(maxlen=self.maxlen)
        )
        self._locks: Dict[Tuple[str, str], asyncio.Lock] = defaultdict(asyncio.Lock)

    # ------------------------------------------------------------------
    # Write API
    # ------------------------------------------------------------------

    async def update(self, event: MetricEvent) -> None:
        """Append a single MetricEvent to the appropriate time series."""
        key = (event.service_name, event.metric_name)
        ts = datetime.fromtimestamp(event.timestamp_ms / 1_000, tz=timezone.utc)
        async with self._locks[key]:
            self._series[key].append((ts, event.value))

        logger.debug(
            "metric ingested service=%s metric=%s type=%s value=%s",
            event.service_name,
            event.metric_name,
            event.metric_type,
            event.value,
        )

    # ------------------------------------------------------------------
    # Read API
    # ------------------------------------------------------------------

    def get_series(
        self,
        service_name: str,
        metric_name: str,
        window_minutes: Optional[int] = None,
    ) -> List[MetricPoint]:
        """Return time-series points for the given (service, metric) pair.

        Parameters
        ----------
        window_minutes:
            If set, only return points within the last N minutes.
        """
        key = (service_name, metric_name)
        entries = list(self._series.get(key, []))

        if window_minutes is not None:
            from datetime import timedelta
            cutoff = datetime.now(tz=timezone.utc) - timedelta(minutes=window_minutes)
            entries = [(ts, v) for ts, v in entries if ts >= cutoff]

        return [MetricPoint(timestamp=ts, value=v) for ts, v in entries]

    def get_metric_names(self, service_name: str) -> List[str]:
        """Return all metric names that have data for *service_name*."""
        return [
            metric_name
            for (svc, metric_name), q in self._series.items()
            if svc == service_name and len(q) > 0
        ]

    def get_services(self) -> List[str]:
        """Return all service names that have at least one data point."""
        return list({svc for (svc, _), q in self._series.items() if len(q) > 0})

    def size(self, service_name: str, metric_name: str) -> int:
        """Number of data-points stored for the (service, metric) pair."""
        return len(self._series.get((service_name, metric_name), []))
