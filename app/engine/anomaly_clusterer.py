"""Time-window anomaly clustering.

Groups anomalies from the same service that arrive within a sliding window into
a single IncidentCluster.  This provides basic "incident" grouping without
requiring a graph database or ML model.

Algorithm
---------
For each incoming anomaly:
  1. Find any open incident for the same service whose last_seen_at is within
     the cluster window (default 5 min).
  2. If found: append this anomaly to the existing incident and extend last_seen_at.
  3. If not found: open a new incident.
"""
from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


@dataclass
class IncidentCluster:
    incident_id: str
    service_name: str
    anomaly_ids: list[str]
    anomaly_types: list[str]
    severity: str          # escalates to "critical" if any member is critical
    started_at: datetime
    last_seen_at: datetime
    open: bool = True


class AnomalyClusterer:
    """Groups anomaly dicts into IncidentClusters.

    Parameters
    ----------
    window_seconds:
        How long an incident stays "open" after its last anomaly.
        Default 300 s (5 minutes).
    resolved_ttl_hours:
        How long closed incidents are retained before purging.
    """

    def __init__(
        self,
        window_seconds: int = 300,
        resolved_ttl_hours: int = 24,
    ) -> None:
        self._window = timedelta(seconds=window_seconds)
        self._ttl = timedelta(hours=resolved_ttl_hours)
        self._incidents: dict[str, IncidentCluster] = {}
        self._lock = asyncio.Lock()

    async def ingest(self, anomaly: dict) -> str:
        """Assign anomaly to an incident and return the incident_id."""
        svc = anomaly.get("service_name", "")
        atype = anomaly.get("anomaly_type", "")
        anom_id = anomaly.get("id", uuid.uuid4().hex)
        severity = anomaly.get("severity", "warning")
        ts: datetime = anomaly.get("minute") or datetime.now(tz=UTC)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)

        async with self._lock:
            open_inc: IncidentCluster | None = None
            for inc in self._incidents.values():
                if (
                    inc.open
                    and inc.service_name == svc
                    and (ts - inc.last_seen_at) <= self._window
                ):
                    open_inc = inc
                    break

            if open_inc is not None:
                open_inc.anomaly_ids.append(anom_id)
                if atype not in open_inc.anomaly_types:
                    open_inc.anomaly_types.append(atype)
                open_inc.last_seen_at = ts
                if severity == "critical":
                    open_inc.severity = "critical"
                return open_inc.incident_id

            inc_id = uuid.uuid4().hex
            self._incidents[inc_id] = IncidentCluster(
                incident_id=inc_id,
                service_name=svc,
                anomaly_ids=[anom_id],
                anomaly_types=[atype],
                severity=severity,
                started_at=ts,
                last_seen_at=ts,
            )
            return inc_id

    async def close_incident(self, incident_id: str) -> IncidentCluster | None:
        async with self._lock:
            inc = self._incidents.get(incident_id)
            if inc:
                inc.open = False
            return inc

    async def list_incidents(self, open_only: bool = True) -> list[IncidentCluster]:
        async with self._lock:
            self._purge_old()
            if open_only:
                return [i for i in self._incidents.values() if i.open]
            return list(self._incidents.values())

    async def get_incident(self, incident_id: str) -> IncidentCluster | None:
        async with self._lock:
            return self._incidents.get(incident_id)

    def _purge_old(self) -> None:
        now = datetime.now(tz=UTC)
        expired = [
            k for k, v in self._incidents.items()
            if not v.open and (now - v.last_seen_at) > self._ttl
        ]
        for k in expired:
            del self._incidents[k]
