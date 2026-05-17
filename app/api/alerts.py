"""Alert lifecycle management API.

GET  /api/alerts                        – list alerts (filter by state / service)
GET  /api/alerts/{alert_id}             – single alert detail
POST /api/alerts/{alert_id}/ack         – FIRING → ACKNOWLEDGED
POST /api/alerts/{alert_id}/resolve     – any → RESOLVED

Also exposes incident clusters from AnomalyClusterer:
GET  /api/alerts/incidents              – list open incident clusters
GET  /api/alerts/incidents/{id}         – single incident detail
POST /api/alerts/incidents/{id}/close   – close an incident manually
"""
from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from ..engine.alert_store import AlertRecord, AlertState

router = APIRouter(prefix="/api/alerts", tags=["alerts"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_alert_store(request: Request):
    store = getattr(request.app.state, "alert_store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="Alert store not initialised")
    return store


def _get_clusterer(request: Request):
    return getattr(request.app.state, "anomaly_clusterer", None)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class AlertResponse(BaseModel):
    id: str
    service_name: str
    span_name: str
    anomaly_type: str
    severity: str
    current_value: float
    baseline_value: float
    z_score: float
    state: str
    fired_at: str
    acked_at: Optional[str] = None
    resolved_at: Optional[str] = None
    ack_by: Optional[str] = None


class IncidentResponse(BaseModel):
    incident_id: str
    service_name: str
    anomaly_ids: List[str]
    anomaly_types: List[str]
    severity: str
    started_at: str
    last_seen_at: str
    open: bool


def _fmt(rec: AlertRecord) -> AlertResponse:
    return AlertResponse(
        id=rec.id,
        service_name=rec.service_name,
        span_name=rec.span_name,
        anomaly_type=rec.anomaly_type,
        severity=rec.severity,
        current_value=rec.current_value,
        baseline_value=rec.baseline_value,
        z_score=rec.z_score,
        state=rec.state.value,
        fired_at=rec.fired_at.isoformat(),
        acked_at=rec.acked_at.isoformat() if rec.acked_at else None,
        resolved_at=rec.resolved_at.isoformat() if rec.resolved_at else None,
        ack_by=rec.ack_by,
    )


# ---------------------------------------------------------------------------
# Alert endpoints
# ---------------------------------------------------------------------------

@router.get("", response_model=List[AlertResponse], summary="List alerts")
async def list_alerts(
    request: Request,
    state: Optional[str] = Query(None, description="FIRING | ACKNOWLEDGED | RESOLVED"),
    service_name: Optional[str] = Query(None),
) -> List[AlertResponse]:
    store = _get_alert_store(request)
    alert_state = AlertState(state) if state else None
    records = await store.list_alerts(state=alert_state, service_name=service_name)
    return [_fmt(r) for r in records]


@router.get("/incidents", response_model=List[IncidentResponse], summary="List incident clusters")
async def list_incidents(
    request: Request,
    open_only: bool = Query(True),
) -> List[IncidentResponse]:
    clusterer = _get_clusterer(request)
    if clusterer is None:
        return []
    incidents = await clusterer.list_incidents(open_only=open_only)
    return [
        IncidentResponse(
            incident_id=i.incident_id,
            service_name=i.service_name,
            anomaly_ids=i.anomaly_ids,
            anomaly_types=i.anomaly_types,
            severity=i.severity,
            started_at=i.started_at.isoformat(),
            last_seen_at=i.last_seen_at.isoformat(),
            open=i.open,
        )
        for i in incidents
    ]


@router.get("/{alert_id}", response_model=AlertResponse, summary="Get alert detail")
async def get_alert(alert_id: str, request: Request) -> AlertResponse:
    store = _get_alert_store(request)
    rec = await store.get_alert(alert_id)
    if rec is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    return _fmt(rec)


@router.post("/{alert_id}/ack", response_model=AlertResponse, summary="Acknowledge alert")
async def ack_alert(
    alert_id: str,
    request: Request,
    ack_by: str = Query("user"),
) -> AlertResponse:
    store = _get_alert_store(request)
    rec = await store.acknowledge(alert_id, ack_by=ack_by)
    if rec is None:
        raise HTTPException(status_code=404, detail="Alert not found or not in FIRING state")
    return _fmt(rec)


@router.post("/{alert_id}/resolve", response_model=AlertResponse, summary="Resolve alert")
async def resolve_alert(alert_id: str, request: Request) -> AlertResponse:
    store = _get_alert_store(request)
    rec = await store.resolve(alert_id)
    if rec is None:
        raise HTTPException(status_code=404, detail="Alert not found or already RESOLVED")
    return _fmt(rec)


@router.post("/incidents/{incident_id}/close", response_model=IncidentResponse, summary="Close incident")
async def close_incident(incident_id: str, request: Request) -> IncidentResponse:
    clusterer = _get_clusterer(request)
    if clusterer is None:
        raise HTTPException(status_code=503, detail="Anomaly clusterer not initialised")
    inc = await clusterer.close_incident(incident_id)
    if inc is None:
        raise HTTPException(status_code=404, detail="Incident not found")
    return IncidentResponse(
        incident_id=inc.incident_id,
        service_name=inc.service_name,
        anomaly_ids=inc.anomaly_ids,
        anomaly_types=inc.anomaly_types,
        severity=inc.severity,
        started_at=inc.started_at.isoformat(),
        last_seen_at=inc.last_seen_at.isoformat(),
        open=inc.open,
    )
