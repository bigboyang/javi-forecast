"""In-memory ring-buffer feature store for RED metrics.

Each service gets a deque of (timestamp, REDMetric) pairs capped at
``maxlen`` entries.  All mutations are protected by a per-service
asyncio.Lock so concurrent updates from the Kafka consumer and the
forecast loop are safe.
"""

import asyncio
import logging
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Dict, Deque, List, Optional, Tuple

from ..models.metric import REDMetric, MetricPoint
from ..models.span import SpanEvent

logger = logging.getLogger(__name__)

# How many 1-minute buckets to keep per service (72 h worth by default).
_DEFAULT_MAXLEN = 4320
# Default maximum distinct services tracked before LRU eviction.
_DEFAULT_MAX_SERVICES = 1000

# Internal per-service accumulator bucket (1-minute resolution).
_METRIC_NAMES = ("rate", "error_rate", "p50_ms", "p95_ms", "p99_ms")


class _Bucket:
    """Accumulates raw spans within a 1-minute window."""

    __slots__ = ("minute_ts", "count", "errors", "durations")

    def __init__(self, minute_ts: datetime) -> None:
        self.minute_ts: datetime = minute_ts
        self.count: int = 0
        self.errors: int = 0
        self.durations: List[float] = []

    def add(self, span: SpanEvent) -> None:
        self.count += 1
        if span.is_error:
            self.errors += 1
        self.durations.append(span.duration_ms)

    def to_red(self, service_name: str) -> REDMetric:
        import numpy as np

        rate = self.count / 60.0  # per-second within the minute
        error_rate = self.errors / self.count if self.count else 0.0
        d = np.array(self.durations, dtype=float)
        p50 = float(np.percentile(d, 50)) if len(d) else 0.0
        p95 = float(np.percentile(d, 95)) if len(d) else 0.0
        p99 = float(np.percentile(d, 99)) if len(d) else 0.0
        return REDMetric(
            service_name=service_name,
            timestamp=self.minute_ts,
            rate=rate,
            error_rate=error_rate,
            p50_ms=p50,
            p95_ms=p95,
            p99_ms=p99,
        )


def _truncate_to_minute(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)


class FeatureStore:
    """Thread-safe in-memory feature store keyed by service name.

    Attributes
    ----------
    maxlen : int
        Maximum number of RED metric data-points kept per service.
    """

    def __init__(
        self,
        maxlen: int = _DEFAULT_MAXLEN,
        max_services: int = _DEFAULT_MAX_SERVICES,
    ) -> None:
        self.maxlen = maxlen
        self._max_services = max_services

        # service → deque[(minute_ts, REDMetric)]
        self._series: Dict[str, Deque[Tuple[datetime, REDMetric]]] = defaultdict(
            lambda: deque(maxlen=self.maxlen)
        )
        # service → current open bucket
        self._buckets: Dict[str, _Bucket] = {}
        # per-service locks
        self._locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        # service → set of ingested minute timestamps (for backfill dedup)
        self._ts_index: Dict[str, set] = defaultdict(set)
        # service → last-access monotonic time (for LRU eviction)
        self._lru: Dict[str, float] = {}

    # ------------------------------------------------------------------
    # Public write API
    # ------------------------------------------------------------------

    async def update(self, span: SpanEvent) -> None:
        """Incorporate a single span event into the feature store."""
        service = span.service_name
        self._touch(service)
        async with self._locks[service]:
            span_ts = datetime.fromtimestamp(
                span.start_time_nano / 1e9, tz=timezone.utc
            )
            minute_ts = _truncate_to_minute(span_ts)

            bucket = self._buckets.get(service)
            if bucket is None or bucket.minute_ts != minute_ts:
                # Flush the old bucket (if any) and open a new one.
                if bucket is not None:
                    self._flush_bucket(service, bucket)
                self._buckets[service] = _Bucket(minute_ts)
                bucket = self._buckets[service]

            bucket.add(span)

    async def ingest_red(self, metric: REDMetric) -> None:
        """Directly insert a pre-computed REDMetric (used during backfill).

        Silently skips duplicate minute timestamps to prevent double-counting
        when backfill and the Kafka consumer overlap in time.
        """
        service = metric.service_name
        self._touch(service)
        async with self._locks[service]:
            minute_ts = _truncate_to_minute(metric.timestamp)
            if minute_ts in self._ts_index[service]:
                return  # deduplicate
            self._ts_index[service].add(minute_ts)
            self._series[service].append((minute_ts, metric))

    async def flush_open_buckets(self) -> None:
        """Close and flush all open accumulation buckets."""
        for service in list(self._buckets.keys()):
            async with self._locks[service]:
                bucket = self._buckets.pop(service, None)
                if bucket and bucket.count > 0:
                    self._flush_bucket(service, bucket)

    # ------------------------------------------------------------------
    # Public read API
    # ------------------------------------------------------------------

    def get_series(
        self,
        service: str,
        metric: str,
        window_minutes: Optional[int] = None,
    ) -> List[MetricPoint]:
        """Return time-series points for *metric* of *service*.

        Parameters
        ----------
        metric:
            One of ``rate | error_rate | p50_ms | p95_ms | p99_ms``.
        window_minutes:
            If set, only return points within the last N minutes.
        """
        if metric not in _METRIC_NAMES:
            raise ValueError(f"Unknown metric '{metric}'. Must be one of {_METRIC_NAMES}")

        entries = list(self._series.get(service, []))
        if window_minutes is not None:
            from datetime import timedelta
            cutoff = datetime.now(tz=timezone.utc) - timedelta(minutes=window_minutes)
            entries = [(ts, red) for ts, red in entries if ts >= cutoff]

        return [
            MetricPoint(timestamp=ts, value=getattr(red, metric))
            for ts, red in entries
        ]

    def get_red_metrics(
        self, service: str, window_minutes: Optional[int] = None
    ) -> List[REDMetric]:
        """Return raw RED metric objects for *service*."""
        entries = list(self._series.get(service, []))
        if window_minutes is not None:
            from datetime import timedelta
            cutoff = datetime.now(tz=timezone.utc) - timedelta(minutes=window_minutes)
            entries = [(ts, red) for ts, red in entries if ts >= cutoff]
        return [red for _, red in entries]

    def get_services(self) -> List[str]:
        """Return all service names that have at least one data point."""
        return [s for s, q in self._series.items() if len(q) > 0]

    def size(self, service: str) -> int:
        """Number of data points currently stored for *service*."""
        return len(self._series.get(service, []))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _touch(self, service: str) -> None:
        """Update LRU timestamp and evict the least-recently-used service
        if ``_max_services`` is exceeded.

        Called **before** acquiring per-service lock to avoid deadlocks.
        """
        now = time.monotonic()
        is_new = service not in self._lru
        self._lru[service] = now
        if is_new and len(self._lru) > self._max_services:
            self._evict_lru()

    def _evict_lru(self) -> None:
        """Remove the least-recently-used service from all internal structures."""
        if not self._lru:
            return
        lru_service = min(self._lru, key=lambda s: self._lru[s])
        logger.warning(
            "FeatureStore cardinality limit reached (%d). "
            "Evicting LRU service '%s'.",
            self._max_services,
            lru_service,
        )
        self._lru.pop(lru_service, None)
        self._series.pop(lru_service, None)
        self._buckets.pop(lru_service, None)
        self._ts_index.pop(lru_service, None)
        self._locks.pop(lru_service, None)

    def _flush_bucket(self, service: str, bucket: _Bucket) -> None:
        if bucket.count == 0:
            return
        red = bucket.to_red(service)
        self._ts_index[service].add(bucket.minute_ts)
        self._series[service].append((bucket.minute_ts, red))
        logger.debug(
            "flushed bucket service=%s ts=%s count=%d",
            service,
            bucket.minute_ts,
            bucket.count,
        )
