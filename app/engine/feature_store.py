"""In-memory ring-buffer feature store for RED metrics.

Each service gets a deque of (timestamp, REDMetric) pairs capped at
``maxlen`` entries.  All mutations are protected by a per-service
asyncio.Lock so concurrent updates from the Kafka consumer and the
forecast loop are safe.

When a Redis client is provided (via ``redis_client``), each completed
1-minute bucket is also written to Redis as a sorted set (score = Unix
timestamp).  On startup, ``load_from_redis()`` repopulates the local
deque from Redis — this survives pod restarts and enables multi-replica
consistency.

Redis key layout
----------------
``fs:services``              -> Redis Set of all known service names
``fs:series:{service}``      -> Redis Sorted Set (score=unix_ts, member=JSON REDMetric)
"""

import asyncio
import logging
import time
from collections import defaultdict, deque
from datetime import UTC, datetime
from typing import Any

from ..models.metric import MetricPoint, REDMetric
from ..models.span import SpanEvent

logger = logging.getLogger(__name__)

# How many 1-minute buckets to keep per service (72 h worth by default).
_DEFAULT_MAXLEN = 4320
# Default maximum distinct services tracked before LRU eviction.
_DEFAULT_MAX_SERVICES = 1000

_REDIS_SERVICES_KEY = "fs:services"
_REDIS_SERIES_PREFIX = "fs:series:"

# Internal per-service accumulator bucket (1-minute resolution).
_METRIC_NAMES = ("rate", "error_rate", "p50_ms", "p95_ms", "p99_ms")


class _Bucket:
    """Accumulates raw spans within a 1-minute window."""

    __slots__ = ("minute_ts", "count", "errors", "durations")

    def __init__(self, minute_ts: datetime) -> None:
        self.minute_ts: datetime = minute_ts
        self.count: int = 0
        self.errors: int = 0
        self.durations: list[float] = []

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

    When ``redis_client`` is provided, completed 1-minute buckets are
    written through to Redis (best-effort) and ``load_from_redis()``
    can repopulate state after a pod restart.

    Attributes
    ----------
    maxlen : int
        Maximum number of RED metric data-points kept per service.
    """

    def __init__(
        self,
        maxlen: int = _DEFAULT_MAXLEN,
        max_services: int = _DEFAULT_MAX_SERVICES,
        redis_client: Any | None = None,
    ) -> None:
        self.maxlen = maxlen
        self._max_services = max_services
        self._redis = redis_client

        # service → deque[(minute_ts, REDMetric)]
        self._series: dict[str, deque[tuple[datetime, REDMetric]]] = defaultdict(
            lambda: deque(maxlen=self.maxlen)
        )
        # service → current open bucket
        self._buckets: dict[str, _Bucket] = {}
        # per-service locks
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        # service → set of ingested minute timestamps (for backfill dedup)
        self._ts_index: dict[str, set] = defaultdict(set)
        # service → last-access monotonic time (for LRU eviction)
        self._lru: dict[str, float] = {}

    # ------------------------------------------------------------------
    # Public write API
    # ------------------------------------------------------------------

    async def update(self, span: SpanEvent) -> None:
        """Incorporate a single span event into the feature store."""
        service = span.service_name
        self._touch(service)
        async with self._locks[service]:
            span_ts = datetime.fromtimestamp(
                span.start_time_nano / 1e9, tz=UTC
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
        await self._redis_persist(service, minute_ts, metric)

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
        window_minutes: int | None = None,
    ) -> list[MetricPoint]:
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
            cutoff = datetime.now(tz=UTC) - timedelta(minutes=window_minutes)
            entries = [(ts, red) for ts, red in entries if ts >= cutoff]

        return [
            MetricPoint(timestamp=ts, value=getattr(red, metric))
            for ts, red in entries
        ]

    def get_red_metrics(
        self, service: str, window_minutes: int | None = None
    ) -> list[REDMetric]:
        """Return raw RED metric objects for *service*."""
        entries = list(self._series.get(service, []))
        if window_minutes is not None:
            from datetime import timedelta
            cutoff = datetime.now(tz=UTC) - timedelta(minutes=window_minutes)
            entries = [(ts, red) for ts, red in entries if ts >= cutoff]
        return [red for _, red in entries]

    def get_services(self) -> list[str]:
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
        # Schedule Redis persist (fire-and-forget; errors are logged, not raised).
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._redis_persist(service, bucket.minute_ts, red))
        except RuntimeError:
            pass  # no running loop during shutdown flush

    # ------------------------------------------------------------------
    # Redis write-through helpers
    # ------------------------------------------------------------------

    async def _redis_persist(
        self, service: str, minute_ts: datetime, metric: REDMetric
    ) -> None:
        """Write a single REDMetric to Redis (best-effort)."""
        if self._redis is None:
            return
        key = f"{_REDIS_SERIES_PREFIX}{service}"
        score = minute_ts.timestamp()
        value = metric.model_dump_json()
        try:
            pipe = self._redis.pipeline()
            pipe.zadd(key, {value: score})
            pipe.sadd(_REDIS_SERVICES_KEY, service)
            # Keep only the newest `maxlen` entries (trim oldest).
            pipe.zremrangebyrank(key, 0, -(self.maxlen + 1))
            await pipe.execute()
        except Exception as exc:
            logger.warning("Redis persist failed service=%s: %s", service, exc)

    async def load_from_redis(self) -> int:
        """Populate in-memory store from Redis on startup.

        Returns the total number of data points loaded across all services.
        Should be called once after the Redis connection is established and
        before the Kafka consumer / backfill starts, so that the dedup
        ``_ts_index`` is already populated when new data arrives.
        """
        if self._redis is None:
            return 0

        loaded = 0
        try:
            raw_services = await self._redis.smembers(_REDIS_SERVICES_KEY)
        except Exception as exc:
            logger.warning("Redis load_from_redis: smembers failed: %s", exc)
            return 0

        for raw_svc in raw_services:
            service = raw_svc.decode() if isinstance(raw_svc, bytes) else raw_svc
            key = f"{_REDIS_SERIES_PREFIX}{service}"
            try:
                entries = await self._redis.zrangebyscore(
                    key, "-inf", "+inf", withscores=True
                )
            except Exception as exc:
                logger.warning("Redis load_from_redis: zrangebyscore failed svc=%s: %s", service, exc)
                continue

            self._touch(service)
            async with self._locks[service]:
                for raw_value, score in entries:
                    raw_value = raw_value.decode() if isinstance(raw_value, bytes) else raw_value
                    try:
                        metric = REDMetric.model_validate_json(raw_value)
                    except Exception:
                        continue
                    minute_ts = datetime.fromtimestamp(score, tz=UTC)
                    if minute_ts in self._ts_index[service]:
                        continue
                    self._ts_index[service].add(minute_ts)
                    self._series[service].append((minute_ts, metric))
                    loaded += 1

        logger.info(
            "FeatureStore loaded %d data points across %d services from Redis",
            loaded,
            len(raw_services),
        )
        return loaded
