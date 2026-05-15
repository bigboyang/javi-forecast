"""In-memory deployment event store.

Keeps the last N deployment events per service for correlation
with anomaly forecasts.  Thread-safe; no persistence (restart clears history).
"""

from __future__ import annotations

import threading
from collections import deque
from datetime import datetime, timezone
from typing import Dict, List, Optional

from ..models.deployment import DeploymentEvent

_DEFAULT_MAXLEN = 200   # max events total
_PER_SERVICE_MAXLEN = 50


class DeploymentStore:
    """Circular buffer of recent deployment events, indexed by service.

    Parameters
    ----------
    maxlen:
        Maximum total events retained (LRU drop on overflow).
    """

    def __init__(self, maxlen: int = _DEFAULT_MAXLEN) -> None:
        self._lock = threading.Lock()
        self._by_service: Dict[str, deque] = {}
        self._all: deque = deque(maxlen=maxlen)

    def add(self, event: DeploymentEvent) -> None:
        with self._lock:
            self._all.append(event)
            svc = event.service_name
            if svc not in self._by_service:
                self._by_service[svc] = deque(maxlen=_PER_SERVICE_MAXLEN)
            self._by_service[svc].append(event)

    def get_by_service(
        self,
        service_name: str,
        since_ms: Optional[int] = None,
        limit: int = 50,
    ) -> List[DeploymentEvent]:
        with self._lock:
            events = list(self._by_service.get(service_name, []))
        if since_ms is not None:
            events = [e for e in events if e.timestamp_ms >= since_ms]
        return events[-limit:]

    def get_all(
        self,
        since_ms: Optional[int] = None,
        limit: int = 200,
    ) -> List[DeploymentEvent]:
        with self._lock:
            events = list(self._all)
        if since_ms is not None:
            events = [e for e in events if e.timestamp_ms >= since_ms]
        return events[-limit:]

    def latest_for_service(self, service_name: str) -> Optional[DeploymentEvent]:
        """Return the most recent deployment for *service_name*, or None."""
        with self._lock:
            q = self._by_service.get(service_name)
            if not q:
                return None
            return q[-1]
