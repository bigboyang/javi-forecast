"""Deployment events API.

GET /v1/events/deploy           – list recent deployment events (all services)
GET /v1/events/deploy/{service} – list deployments for a specific service
POST /v1/events/deploy          – ingest a deployment event directly (no Kafka)
"""

from __future__ import annotations

import logging
import time
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query, Request, status

from ..models.deployment import DeploymentEvent

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/events", tags=["events"])


def _get_store(request: Request):
    store = getattr(request.app.state, "deployment_store", None)
    if store is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="DeploymentStore is not initialised",
        )
    return store


@router.get(
    "/deploy",
    response_model=List[DeploymentEvent],
    summary="List recent deployment events across all services",
)
async def list_deployments(
    request: Request,
    since_ms: Optional[int] = Query(None, description="Unix ms lower bound"),
    limit: int = Query(100, ge=1, le=500),
) -> List[DeploymentEvent]:
    store = _get_store(request)
    return store.get_all(since_ms=since_ms, limit=limit)


@router.get(
    "/deploy/{service_name}",
    response_model=List[DeploymentEvent],
    summary="List deployment events for a specific service",
)
async def list_service_deployments(
    service_name: str,
    request: Request,
    since_ms: Optional[int] = Query(None, description="Unix ms lower bound"),
    limit: int = Query(50, ge=1, le=200),
) -> List[DeploymentEvent]:
    store = _get_store(request)
    events = store.get_by_service(service_name, since_ms=since_ms, limit=limit)
    return events


@router.post(
    "/deploy",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest a deployment event directly (bypasses Kafka)",
)
async def ingest_deployment(
    event: DeploymentEvent,
    request: Request,
) -> dict:
    """Direct HTTP fallback when Kafka is disabled.

    In production, deployment events should flow via javi-collector → Kafka.
    This endpoint is useful for local dev or non-Kafka setups.
    """
    store = _get_store(request)
    deploy_handler = getattr(request.app.state, "deploy_event_handler", None)
    if deploy_handler is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="DeployEventHandler is not initialised",
        )
    if event.timestamp_ms == 0:
        event.timestamp_ms = int(time.time() * 1000)
    await deploy_handler.handle(event)
    return {"status": "accepted"}
