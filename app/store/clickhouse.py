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


    # ------------------------------------------------------------------
    # Anomaly / RCA write helpers (ported from collector Go anomaly pkg)
    # ------------------------------------------------------------------

    async def _command(self, sql: str, data: Optional[list] = None) -> None:
        """Execute a write *sql* statement (INSERT / CREATE …)."""
        if self._client is None:
            raise RuntimeError("ClickHouseStore not connected")
        loop = asyncio.get_event_loop()
        if data is not None:
            await loop.run_in_executor(None, lambda: self._client.insert(sql, data))
        else:
            await loop.run_in_executor(None, lambda: self._client.command(sql))

    async def insert_anomaly(self, row: dict) -> None:
        """Insert a single anomaly record into apm.anomalies.

        Expected keys: id, service_name, span_name, anomaly_type, minute,
                       current_value, baseline_value, z_score, severity
        """
        sql = f"INSERT INTO {self._database}.anomalies"
        await loop_run(self._client, sql, [row])

    async def insert_anomalies(self, rows: list[dict]) -> None:
        """Batch-insert anomaly records."""
        if not rows:
            return
        loop = asyncio.get_event_loop()
        sql = f"INSERT INTO {self._database}.anomalies"
        await loop.run_in_executor(None, lambda: self._client.insert(sql, rows))

    async def insert_rca_reports(self, rows: list[dict]) -> None:
        """Batch-insert RCA report records."""
        if not rows:
            return
        loop = asyncio.get_event_loop()
        sql = f"INSERT INTO {self._database}.rca_reports"
        await loop.run_in_executor(None, lambda: self._client.insert(sql, rows))

    async def compute_baseline(self) -> None:
        """Run the red_baseline INSERT…SELECT aggregation over the last 28 days."""
        sql = f"""
INSERT INTO {self._database}.red_baseline
    (service_name, span_name, http_route,
     day_of_week, hour_of_day,
     p50_ms, p95_ms, p99_ms,
     error_rate, avg_rps, sample_count,
     computed_at, dt)
SELECT
    service_name,
    name                                                                    AS span_name,
    http_route,
    toDayOfWeek(fromUnixTimestamp64Nano(start_time_nano))                  AS day_of_week,
    toHour(fromUnixTimestamp64Nano(start_time_nano))                        AS hour_of_day,
    quantile(0.50)(duration_nano) / 1e6                                     AS p50_ms,
    quantile(0.95)(duration_nano) / 1e6                                     AS p95_ms,
    quantile(0.99)(duration_nano) / 1e6                                     AS p99_ms,
    toFloat64(countIf(status_code = 2)) / count()                           AS error_rate,
    toFloat64(count()) /
        (toFloat64(countDistinct(toDate(fromUnixTimestamp64Nano(start_time_nano)))) * 3600.0)
                                                                            AS avg_rps,
    toUInt64(count())                                                        AS sample_count,
    now()                                                                    AS computed_at,
    today()                                                                  AS dt
FROM {self._database}.spans
WHERE kind IN (2, 5)
  AND dt >= today() - 28
  AND dt <  today()
GROUP BY service_name, span_name, http_route, day_of_week, hour_of_day
HAVING sample_count >= 10
"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: self._client.command(sql))

    async def query_current_red(self) -> list[dict]:
        """Return RED metrics for the last completed minute from mv_red_1m_state."""
        sql = f"""
SELECT
    service_name,
    span_name,
    http_route,
    minute,
    sum(total_count)                                                                            AS req_count,
    if(sum(total_count) > 0, toFloat64(sum(error_count)) / toFloat64(sum(total_count)), 0)    AS error_rate,
    quantilesMerge(0.5, 0.95, 0.99)(duration_quantiles)[1] / 1e6                              AS p50_ms,
    quantilesMerge(0.5, 0.95, 0.99)(duration_quantiles)[2] / 1e6                              AS p95_ms,
    quantilesMerge(0.5, 0.95, 0.99)(duration_quantiles)[3] / 1e6                              AS p99_ms
FROM {self._database}.mv_red_1m_state
WHERE minute = toStartOfMinute(now()) - INTERVAL 1 MINUTE
GROUP BY service_name, span_name, http_route, minute
HAVING req_count >= 5
"""
        result = await self._query(sql)
        rows = []
        for r in result.result_rows:
            rows.append({
                "service_name": r[0],
                "span_name": r[1],
                "http_route": r[2],
                "minute": _ensure_utc(r[3]),
                "req_count": int(r[4]),
                "error_rate": float(r[5]),
                "p50_ms": float(r[6]),
                "p95_ms": float(r[7]),
                "p99_ms": float(r[8]),
            })
        return rows

    async def query_baseline_all(self) -> list[dict]:
        """Load all red_baseline rows (used for IsolationForest training)."""
        sql = f"""
SELECT
    service_name, span_name, http_route,
    day_of_week, hour_of_day,
    p50_ms, p95_ms, p99_ms,
    error_rate, avg_rps, sample_count
FROM {self._database}.red_baseline FINAL
"""
        result = await self._query(sql)
        rows = []
        for r in result.result_rows:
            rows.append({
                "service_name": r[0], "span_name": r[1], "http_route": r[2],
                "day_of_week": int(r[3]), "hour_of_day": int(r[4]),
                "p50_ms": float(r[5]), "p95_ms": float(r[6]), "p99_ms": float(r[7]),
                "error_rate": float(r[8]), "avg_rps": float(r[9]),
                "sample_count": int(r[10]),
            })
        return rows

    async def query_anomalies_recent(self, window_seconds: int) -> list[dict]:
        """Return anomalies detected within the last *window_seconds*."""
        sql = f"""
SELECT id, service_name, span_name, anomaly_type, minute,
       current_value, baseline_value, z_score, severity, detected_at
FROM {self._database}.anomalies
WHERE detected_at >= now() - INTERVAL {window_seconds} SECOND
ORDER BY detected_at ASC
"""
        result = await self._query(sql)
        rows = []
        for r in result.result_rows:
            rows.append({
                "id": r[0], "service_name": r[1], "span_name": r[2],
                "anomaly_type": r[3], "minute": _ensure_utc(r[4]),
                "current_value": float(r[5]), "baseline_value": float(r[6]),
                "z_score": float(r[7]), "severity": r[8],
                "detected_at": _ensure_utc(r[9]),
            })
        return rows

    async def query_correlated_spans(
        self, service_name: str, minute: datetime
    ) -> list[dict]:
        """Return up to 5 ERROR/WARN spans around *minute* for *service_name*."""
        minute_str = minute.strftime("%Y-%m-%d %H:%M:%S")
        sql = f"""
SELECT
    span_id, trace_id, name, status_code, status_message,
    (end_time_nano - start_time_nano) / 1e6 AS duration_ms,
    ifNull(attributes['exception.type'], '')
FROM {self._database}.spans
WHERE service_name = {{svc:String}}
  AND start_time_nano >= toUnixTimestamp64Nano(toDateTime64({{ts:String}}, 9))
  AND start_time_nano <  toUnixTimestamp64Nano(toDateTime64({{ts:String}}, 9)) + 120000000000
  AND status_code >= 2
ORDER BY duration_ms DESC
LIMIT 5
"""
        result = await self._query(sql, {"svc": service_name, "ts": minute_str})
        rows = []
        for r in result.result_rows:
            rows.append({
                "span_id": r[0], "trace_id": r[1], "name": r[2],
                "status_code": int(r[3]), "status_message": r[4],
                "duration_ms": float(r[5]), "exception_type": r[6],
            })
        return rows

    async def query_topology_neighbors(
        self, service_name: str, from_ts: datetime, to_ts: datetime
    ) -> list[dict]:
        """Return direct topology neighbors (upstream + downstream) for *service_name*."""
        sql = f"""
SELECT peer_service, 'downstream' AS direction,
       sum(call_count) AS total_calls,
       if(sum(call_count) > 0,
          toFloat64(sum(error_count)) / toFloat64(sum(call_count)), 0) AS error_rate
FROM {self._database}.mv_service_topology_state
WHERE caller_service = {{svc:String}}
  AND minute >= {{from_ts:DateTime}} AND minute <= {{to_ts:DateTime}}
GROUP BY peer_service
UNION ALL
SELECT caller_service, 'upstream' AS direction,
       sum(call_count) AS total_calls,
       if(sum(call_count) > 0,
          toFloat64(sum(error_count)) / toFloat64(sum(call_count)), 0) AS error_rate
FROM {self._database}.mv_service_topology_state
WHERE peer_service = {{svc:String}}
  AND minute >= {{from_ts:DateTime}} AND minute <= {{to_ts:DateTime}}
GROUP BY caller_service
ORDER BY total_calls DESC
LIMIT 10
"""
        result = await self._query(sql, {"svc": service_name, "from_ts": from_ts, "to_ts": to_ts})
        rows = []
        for r in result.result_rows:
            rows.append({
                "service_name": r[0], "direction": r[1],
                "call_count": int(r[2]), "error_rate": float(r[3]),
            })
        return rows

    async def query_nearby_deployments(
        self, service_name: str, minute: datetime, window_minutes: int = 5
    ) -> list[dict]:
        """Return deployment events within *window_minutes* of *minute*."""
        from_ts = minute - timedelta(minutes=window_minutes)
        to_ts = minute + timedelta(minutes=window_minutes)
        sql = f"""
SELECT id, service_name, version, environment, deployed_by, description, deployed_at
FROM {self._database}.deployment_events
WHERE service_name = {{svc:String}}
  AND deployed_at >= {{from_ts:DateTime}} AND deployed_at <= {{to_ts:DateTime}}
ORDER BY deployed_at DESC
LIMIT 5
"""
        result = await self._query(sql, {"svc": service_name, "from_ts": from_ts, "to_ts": to_ts})
        rows = []
        for r in result.result_rows:
            rows.append({
                "id": r[0], "service_name": r[1], "version": r[2],
                "environment": r[3], "deployed_by": r[4],
                "description": r[5], "deployed_at": _ensure_utc(r[6]),
            })
        return rows


def _ensure_utc(ts) -> datetime:
    """Ensure *ts* (datetime or similar) is timezone-aware UTC."""
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts
    return datetime.now(tz=timezone.utc)


def loop_run(client, sql: str, rows: list) -> None:
    """Synchronous helper used inside run_in_executor."""
    client.insert(sql, rows)
