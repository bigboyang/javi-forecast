"""In-memory alert lifecycle store.

Manages the FIRING → ACKNOWLEDGED → RESOLVED state machine for anomaly alerts.
Every anomaly that passes the WebhookAlerter cooldown creates an AlertRecord here.
"""
from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Dict, List, Optional


class AlertState(str, Enum):
    FIRING = "FIRING"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    RESOLVED = "RESOLVED"


@dataclass
class AlertRecord:
    id: str
    service_name: str
    span_name: str
    anomaly_type: str
    severity: str
    current_value: float
    baseline_value: float
    z_score: float
    state: AlertState
    fired_at: datetime
    acked_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None
    ack_by: Optional[str] = None


class AlertStore:
    """Thread-safe, in-memory alert lifecycle store.

    Parameters
    ----------
    ttl_hours:
        How long to retain RESOLVED alerts before purging them.
    """

    def __init__(self, ttl_hours: int = 24) -> None:
        self._alerts: Dict[str, AlertRecord] = {}
        self._lock = asyncio.Lock()
        self._ttl = timedelta(hours=ttl_hours)

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------

    async def create_alert(self, anomaly: dict) -> AlertRecord:
        """Create a FIRING alert record from an anomaly dict."""
        record = AlertRecord(
            id=uuid.uuid4().hex,
            service_name=anomaly.get("service_name", ""),
            span_name=anomaly.get("span_name", ""),
            anomaly_type=anomaly.get("anomaly_type", ""),
            severity=anomaly.get("severity", "warning"),
            current_value=float(anomaly.get("current_value", 0)),
            baseline_value=float(anomaly.get("baseline_value", 0)),
            z_score=float(anomaly.get("z_score", 0)),
            state=AlertState.FIRING,
            fired_at=datetime.now(tz=timezone.utc),
        )
        async with self._lock:
            self._alerts[record.id] = record
        return record

    async def acknowledge(self, alert_id: str, ack_by: str = "user") -> Optional[AlertRecord]:
        """Transition FIRING → ACKNOWLEDGED. Returns None if not found or wrong state."""
        async with self._lock:
            rec = self._alerts.get(alert_id)
            if rec is None or rec.state != AlertState.FIRING:
                return None
            rec.state = AlertState.ACKNOWLEDGED
            rec.acked_at = datetime.now(tz=timezone.utc)
            rec.ack_by = ack_by
            return rec

    async def resolve(self, alert_id: str) -> Optional[AlertRecord]:
        """Transition any non-RESOLVED state → RESOLVED. Returns None if not found."""
        async with self._lock:
            rec = self._alerts.get(alert_id)
            if rec is None or rec.state == AlertState.RESOLVED:
                return None
            rec.state = AlertState.RESOLVED
            rec.resolved_at = datetime.now(tz=timezone.utc)
            return rec

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    async def get_alert(self, alert_id: str) -> Optional[AlertRecord]:
        async with self._lock:
            return self._alerts.get(alert_id)

    async def list_alerts(
        self,
        state: Optional[AlertState] = None,
        service_name: Optional[str] = None,
    ) -> List[AlertRecord]:
        async with self._lock:
            self._purge_expired()
            records = list(self._alerts.values())

        if state is not None:
            records = [r for r in records if r.state == state]
        if service_name is not None:
            records = [r for r in records if r.service_name == service_name]
        return sorted(records, key=lambda r: r.fired_at, reverse=True)

    def count_firing(self) -> int:
        return sum(1 for r in self._alerts.values() if r.state == AlertState.FIRING)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _purge_expired(self) -> None:
        now = datetime.now(tz=timezone.utc)
        expired = [
            k for k, v in self._alerts.items()
            if v.state == AlertState.RESOLVED
            and v.resolved_at is not None
            and (now - v.resolved_at) > self._ttl
        ]
        for k in expired:
            del self._alerts[k]
