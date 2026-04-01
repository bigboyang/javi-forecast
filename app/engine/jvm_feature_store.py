"""In-memory ring-buffer store for JVM metric snapshots.

Stores JvmMetricEvent snapshots from javi-agent's JvmSnapshotSender.
Each service gets a deque capped at ``maxlen`` entries.
All mutations are protected by a per-service asyncio.Lock.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta
from typing import Deque, Dict, List, Optional

from ..models.jvm import JvmMetricEvent

logger = logging.getLogger(__name__)

_DEFAULT_MAXLEN = 4320  # 72 h at 1-min cadence


class JvmFeatureStore:
    """Thread-safe ring-buffer keyed by service_name.

    Parameters
    ----------
    maxlen:
        Maximum number of snapshots kept per service.
    """

    def __init__(self, maxlen: int = _DEFAULT_MAXLEN) -> None:
        self.maxlen = maxlen
        # service_name → deque[JvmMetricEvent]
        self._series: Dict[str, Deque[JvmMetricEvent]] = defaultdict(
            lambda: deque(maxlen=self.maxlen)
        )
        self._locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def update(self, event: JvmMetricEvent) -> None:
        """Append a single JVM metric snapshot."""
        async with self._locks[event.service_name]:
            self._series[event.service_name].append(event)
        logger.debug(
            "jvm snapshot ingested service=%s heap_used=%d thread_count=%d",
            event.service_name,
            event.heap_used_bytes,
            event.thread_count,
        )

    def get_snapshots(
        self,
        service_name: str,
        window_minutes: Optional[int] = None,
    ) -> List[JvmMetricEvent]:
        """Return JVM snapshots for *service_name*.

        Parameters
        ----------
        window_minutes:
            If set, only return snapshots within the last N minutes.
        """
        entries = list(self._series.get(service_name, []))
        if window_minutes is not None:
            cutoff_ns = int(
                (datetime.now(tz=timezone.utc) - timedelta(minutes=window_minutes))
                .timestamp() * 1e9
            )
            entries = [e for e in entries if e.timestamp_nano >= cutoff_ns]
        return entries

    def get_latest(self, service_name: str) -> Optional[JvmMetricEvent]:
        """Return the most recent snapshot for *service_name*, or None."""
        series = self._series.get(service_name)
        if not series:
            return None
        return series[-1]

    def get_services(self) -> List[str]:
        """Return all service names that have at least one snapshot."""
        return [s for s, q in self._series.items() if len(q) > 0]

    def size(self, service_name: str) -> int:
        """Number of snapshots stored for *service_name*."""
        return len(self._series.get(service_name, []))
