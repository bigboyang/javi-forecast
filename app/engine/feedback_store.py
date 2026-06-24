"""In-memory alert feedback store.

Persists operator-supplied feedback (false-positive / false-negative) for
anomaly alerts so the system can:
- Surface suppression rules for noisy alerts.
- Feed future model calibration (P2 roadmap).

Entries expire after ``ttl_seconds`` (default 7 days) to prevent unbounded
memory growth.
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass
class FeedbackEntry:
    id: str
    service_name: str
    metric_name: str
    severity: str
    is_false_positive: bool
    note: str | None
    created_at: datetime
    _expire_mono: float = field(repr=False, compare=False, default=0.0)


class FeedbackStore:
    """Thread-safe in-memory store for alert feedback entries.

    Parameters
    ----------
    ttl_seconds:
        How long to retain each feedback entry (default: 7 days).
    """

    def __init__(self, ttl_seconds: int = 7 * 86400) -> None:
        self._ttl = ttl_seconds
        self._entries: dict[str, FeedbackEntry] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def add(
        self,
        service_name: str,
        metric_name: str,
        severity: str,
        is_false_positive: bool,
        note: str | None = None,
    ) -> FeedbackEntry:
        """Record operator feedback and return the stored entry."""
        entry = FeedbackEntry(
            id=str(uuid.uuid4()),
            service_name=service_name,
            metric_name=metric_name,
            severity=severity,
            is_false_positive=is_false_positive,
            note=note,
            created_at=datetime.now(tz=UTC),
            _expire_mono=time.monotonic() + self._ttl,
        )
        with self._lock:
            self._entries[entry.id] = entry
        return entry

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get_all(
        self,
        service_name: str | None = None,
        metric_name: str | None = None,
    ) -> list[FeedbackEntry]:
        """Return non-expired entries, optionally filtered."""
        now = time.monotonic()
        with self._lock:
            # Evict expired
            expired = [eid for eid, e in self._entries.items() if e._expire_mono < now]
            for eid in expired:
                del self._entries[eid]

            entries = list(self._entries.values())

        if service_name:
            entries = [e for e in entries if e.service_name == service_name]
        if metric_name:
            entries = [e for e in entries if e.metric_name == metric_name]
        return sorted(entries, key=lambda e: e.created_at, reverse=True)

    def stats(self) -> dict[str, int]:
        """Return aggregate false-positive / false-negative counts per service."""
        now = time.monotonic()
        fp: dict[str, int] = {}
        fn: dict[str, int] = {}
        with self._lock:
            for e in self._entries.values():
                if e._expire_mono < now:
                    continue
                bucket = fp if e.is_false_positive else fn
                bucket[e.service_name] = bucket.get(e.service_name, 0) + 1
        return {
            "total": len(self._entries),
            "false_positives_by_service": fp,
            "false_negatives_by_service": fn,
        }
