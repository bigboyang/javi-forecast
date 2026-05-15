"""RED baseline computer (ported from javi-collector Go BaselineComputer).

Runs a background asyncio task that periodically executes an
INSERT INTO red_baseline SELECT FROM spans aggregation over the last 28 days.

The red_baseline table uses ReplacingMergeTree(computed_at) so each run
upserts the freshest row per (service, span, route, day_of_week, hour_of_day).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class BaselineComputer:
    """Async background task that keeps red_baseline up to date.

    Parameters
    ----------
    clickhouse:
        ClickHouseStore instance (must be connected before start()).
    interval_hours:
        How often to recompute the baseline (default 1 h).
    """

    def __init__(self, clickhouse, interval_hours: int = 1) -> None:
        self._ch = clickhouse
        self._interval = interval_hours * 3600
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="baseline-computer")
        logger.info("BaselineComputer started interval_hours=%d", self._interval // 3600)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("BaselineComputer stopped")

    async def _run(self) -> None:
        # compute immediately on startup then every interval
        await self._compute()
        while True:
            await asyncio.sleep(self._interval)
            await self._compute()

    async def _compute(self) -> None:
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._ch._client.command, self._build_sql())
            logger.info("red_baseline updated")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("baseline compute failed: %s", exc)

    def _build_sql(self) -> str:
        db = self._ch._database
        return f"""
INSERT INTO {db}.red_baseline
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
FROM {db}.spans
WHERE kind IN (2, 5)
  AND dt >= today() - 28
  AND dt <  today()
GROUP BY service_name, span_name, http_route, day_of_week, hour_of_day
HAVING sample_count >= 10
"""
