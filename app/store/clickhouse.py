"""Async ClickHouse client wrapper.

Uses ``clickhouse-connect`` (HTTP/native async) to query the APM
ClickHouse database.  All methods return Pydantic models so the rest of
the application does not need to know about raw SQL.

Tables assumed to exist
-----------------------
* ``apm.spans``       – raw OTel spans
* ``apm.red_baseline`` – pre-aggregated 1-min RED metrics per service
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from ..config import settings
from ..models.metric import REDMetric

logger = logging.getLogger(__name__)


class ClickHouseStore:
    """Thin async wrapper around clickhouse-connect.

    Parameters
    ----------
    host, port, database, user, password:
        Connection parameters (default from settings).
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        self._host = host or settings.CLICKHOUSE_HOST
        self._port = port or settings.CLICKHOUSE_PORT
        self._database = database or settings.CLICKHOUSE_DB
        self._user = user or settings.CLICKHOUSE_USER
        self._password = password or settings.CLICKHOUSE_PASSWORD
        self._client = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Open the async ClickHouse connection."""
        import clickhouse_connect

        loop = asyncio.get_event_loop()
        self._client = await loop.run_in_executor(
            None,
            lambda: clickhouse_connect.get_client(
                host=self._host,
                port=self._port,
                database=self._database,
                username=self._user,
                password=self._password,
                connect_timeout=10,
                query_retries=2,
            ),
        )
        logger.info(
            "ClickHouse connected host=%s port=%d db=%s",
            self._host,
            self._port,
            self._database,
        )

    async def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None

    async def ping(self) -> bool:
        """Return True when ClickHouse is reachable."""
        if self._client is None:
            return False
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, self._client.ping)
            return True
        except Exception as exc:
            logger.warning("ClickHouse ping failed: %s", exc)
            return False

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    async def _query(self, sql: str, params: Optional[dict] = None):
        """Execute *sql* and return a clickhouse_connect QueryResult."""
        if self._client is None:
            raise RuntimeError("ClickHouseStore not connected – call connect() first")
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            lambda: self._client.query(sql, parameters=params or {}),
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def query_services(self) -> List[str]:
        """Return the list of distinct service names seen in the last 24 h."""
        sql = """
            SELECT DISTINCT service_name
            FROM apm.spans
            WHERE start_time_nano >= toInt64(now() - INTERVAL 24 HOUR) * 1000000000
            ORDER BY service_name
        """
        result = await self._query(sql)
        return [row[0] for row in result.result_rows]

    async def query_red_metrics(
        self,
        service: str,
        from_ts: datetime,
        to_ts: datetime,
    ) -> List[REDMetric]:
        """Return 1-minute RED metrics for *service* in [from_ts, to_ts].

        Falls back to computing from ``apm.spans`` when
        ``apm.red_baseline`` is unavailable.
        """
        try:
            return await self._query_red_baseline(service, from_ts, to_ts)
        except Exception as exc:
            logger.warning(
                "red_baseline query failed, falling back to spans: %s", exc
            )
            return await self._query_red_from_spans(service, from_ts, to_ts)

    async def _query_red_baseline(
        self,
        service: str,
        from_ts: datetime,
        to_ts: datetime,
    ) -> List[REDMetric]:
        # Use mv_red_1m_state (1-minute aggregated MV) as the primary source.
        # apm.red_baseline only stores hourly baselines without time-series data.
        sql = """
            SELECT
                service_name,
                minute                                                              AS ts,
                sum(total_count) / 60.0                                            AS rate,
                if(sum(total_count) > 0,
                   toFloat64(sum(error_count)) / toFloat64(sum(total_count)),
                   0.0)                                                            AS error_rate,
                quantilesMerge(0.5, 0.95, 0.99)(duration_quantiles)[1] / 1e6     AS p50_ms,
                quantilesMerge(0.5, 0.95, 0.99)(duration_quantiles)[2] / 1e6     AS p95_ms,
                quantilesMerge(0.5, 0.95, 0.99)(duration_quantiles)[3] / 1e6     AS p99_ms
            FROM apm.mv_red_1m_state
            WHERE service_name = {service:String}
              AND minute >= {from_ts:DateTime}
              AND minute <= {to_ts:DateTime}
            GROUP BY service_name, ts
            ORDER BY ts ASC
        """
        result = await self._query(
            sql,
            {"service": service, "from_ts": from_ts, "to_ts": to_ts},
        )
        metrics: List[REDMetric] = []
        for row in result.result_rows:
            metrics.append(
                REDMetric(
                    service_name=str(row[0]),
                    timestamp=_ensure_utc(row[1]),
                    rate=float(row[2]),
                    error_rate=float(row[3]),
                    p50_ms=float(row[4]),
                    p95_ms=float(row[5]),
                    p99_ms=float(row[6]),
                )
            )
        return metrics

    async def _query_red_from_spans(
        self,
        service: str,
        from_ts: datetime,
        to_ts: datetime,
    ) -> List[REDMetric]:
        """Compute RED metrics directly from raw spans (slower fallback)."""
        sql = """
            SELECT
                service_name,
                toStartOfMinute(toDateTime(intDiv(start_time_nano, 1000000000))) AS ts,
                count()                                           AS total,
                countIf(is_error = 1)                            AS errors,
                quantile(0.50)(
                    (end_time_nano - start_time_nano) / 1e6
                )                                                AS p50_ms,
                quantile(0.95)(
                    (end_time_nano - start_time_nano) / 1e6
                )                                                AS p95_ms,
                quantile(0.99)(
                    (end_time_nano - start_time_nano) / 1e6
                )                                                AS p99_ms
            FROM apm.spans
            WHERE service_name = {service:String}
              AND toDateTime(intDiv(start_time_nano, 1000000000)) >= {from_ts:DateTime}
              AND toDateTime(intDiv(start_time_nano, 1000000000)) <= {to_ts:DateTime}
            GROUP BY service_name, ts
            ORDER BY ts ASC
        """
        result = await self._query(
            sql,
            {"service": service, "from_ts": from_ts, "to_ts": to_ts},
        )
        metrics: List[REDMetric] = []
        for row in result.result_rows:
            total = int(row[2])
            errors = int(row[3])
            metrics.append(
                REDMetric(
                    service_name=str(row[0]),
                    timestamp=_ensure_utc(row[1]),
                    rate=float(total) / 60.0,
                    error_rate=float(errors) / float(total) if total else 0.0,
                    p50_ms=float(row[4]),
                    p95_ms=float(row[5]),
                    p99_ms=float(row[6]),
                )
            )
        return metrics

    async def backfill_feature_store(self, feature_store) -> None:
        """Load the last ``BACKFILL_HOURS`` of RED metrics into *feature_store*.

        Iterates over all services and feeds pre-computed RED metrics
        directly into the feature store to warm it up on startup.
        """
        if not settings.BACKFILL_ENABLED:
            logger.info("Backfill disabled – skipping")
            return

        to_ts = datetime.now(tz=timezone.utc)
        from_ts = to_ts - timedelta(hours=settings.BACKFILL_HOURS)

        try:
            services = await self.query_services()
        except Exception as exc:
            logger.error("Backfill: failed to list services: %s", exc)
            return

        logger.info(
            "Backfill starting: %d services from=%s to=%s",
            len(services),
            from_ts.isoformat(),
            to_ts.isoformat(),
        )
        total = 0
        for service in services:
            try:
                metrics = await self.query_red_metrics(service, from_ts, to_ts)
                for m in metrics:
                    await feature_store.ingest_red(m)
                total += len(metrics)
                logger.debug(
                    "Backfill service=%s points=%d", service, len(metrics)
                )
            except Exception as exc:
                logger.error(
                    "Backfill failed for service=%s: %s", service, exc
                )

        logger.info("Backfill complete: total_points=%d", total)


def _ensure_utc(ts) -> datetime:
    """Ensure *ts* (datetime or similar) is timezone-aware UTC."""
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts
    # Assume it's a string or numeric – best-effort parse
    return datetime.now(tz=timezone.utc)
