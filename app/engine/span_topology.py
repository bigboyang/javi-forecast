"""Real-time span topology tracker.

Builds a service-to-service call graph from OTel span ``parent_span_id``
relationships observed on the Kafka span topic.

How it works
------------
Every span carries a ``span_id`` and optionally a ``parent_span_id``.
When a span with a parent arrives:
  1. We look up the parent span in a short-lived ``_span_index``
     (span_id → service_name, expire_ts).
  2. If the parent belongs to a *different* service we record a directed
     call edge:  parent_service → child_service.

The resulting call graph is exposed via ``get_topology()`` and includes
per-edge call counts and last-seen timestamps.

Thread safety
-------------
All mutations are protected by ``threading.Lock`` so this class can be
called from asyncio tasks without issues.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from ..models.span import SpanEvent

# How long (seconds) to keep a span_id in the look-up index.
_SPAN_TTL_SECONDS = 120
# Evict the index when it grows past this many entries.
_MAX_INDEX_SIZE = 200_000
# Evict call-count entries older than this many seconds.
_EDGE_TTL_SECONDS = 3600


@dataclass
class TopologyEdge:
    """A directed real-time call edge derived from span parent relationships."""

    caller: str
    callee: str
    call_count: int = 0
    error_count: int = 0
    last_seen: datetime = field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )


class SpanTopologyTracker:
    """Builds a live service call graph from span parent_span_id links.

    Parameters
    ----------
    span_ttl_seconds:
        How long to keep a span in the look-up index.
    edge_ttl_seconds:
        Edges not updated within this window are removed on the next GC.
    max_index_size:
        Trigger index eviction when the span index exceeds this size.
    """

    def __init__(
        self,
        span_ttl_seconds: int = _SPAN_TTL_SECONDS,
        edge_ttl_seconds: int = _EDGE_TTL_SECONDS,
        max_index_size: int = _MAX_INDEX_SIZE,
    ) -> None:
        self._span_ttl = span_ttl_seconds
        self._edge_ttl = edge_ttl_seconds
        self._max_index_size = max_index_size

        # span_id → (service_name, expire_monotonic)
        self._span_index: Dict[str, Tuple[str, float]] = {}
        # (caller, callee) → TopologyEdge
        self._edges: Dict[Tuple[str, str], TopologyEdge] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Write API
    # ------------------------------------------------------------------

    def record(self, span: SpanEvent) -> None:
        """Incorporate *span* into the topology graph."""
        now_mono = time.monotonic()
        expire = now_mono + self._span_ttl

        with self._lock:
            # Evict stale span index entries if needed
            if len(self._span_index) >= self._max_index_size:
                self._span_index = {
                    sid: (svc, exp)
                    for sid, (svc, exp) in self._span_index.items()
                    if exp > now_mono
                }

            # Register this span
            self._span_index[span.span_id] = (span.service_name, expire)

            # Check parent
            if span.parent_span_id:
                parent_info = self._span_index.get(span.parent_span_id)
                if parent_info is not None:
                    parent_service, _exp = parent_info
                    if parent_service != span.service_name:
                        key = (parent_service, span.service_name)
                        edge = self._edges.get(key)
                        if edge is None:
                            edge = TopologyEdge(
                                caller=parent_service,
                                callee=span.service_name,
                            )
                            self._edges[key] = edge
                        edge.call_count += 1
                        if span.is_error:
                            edge.error_count += 1
                        edge.last_seen = datetime.now(tz=timezone.utc)

    # ------------------------------------------------------------------
    # Read API
    # ------------------------------------------------------------------

    def get_topology(self) -> List[TopologyEdge]:
        """Return a snapshot of all live topology edges."""
        now = datetime.now(tz=timezone.utc)
        cutoff_seconds = self._edge_ttl
        with self._lock:
            # GC stale edges
            stale = [
                key
                for key, edge in self._edges.items()
                if (now - edge.last_seen).total_seconds() > cutoff_seconds
            ]
            for key in stale:
                del self._edges[key]
            return list(self._edges.values())

    def get_callers(self, service: str) -> List[TopologyEdge]:
        """Return edges where *service* is the callee (downstream)."""
        with self._lock:
            return [e for e in self._edges.values() if e.callee == service]

    def get_callees(self, service: str) -> List[TopologyEdge]:
        """Return edges where *service* is the caller (upstream)."""
        with self._lock:
            return [e for e in self._edges.values() if e.caller == service]

    def index_size(self) -> int:
        with self._lock:
            return len(self._span_index)

    def edge_count(self) -> int:
        with self._lock:
            return len(self._edges)
