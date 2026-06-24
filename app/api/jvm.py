"""JVM health query endpoints.

GET /api/jvm/services              – list services with JVM data
GET /api/jvm/health/{service}      – latest JVM snapshot for a service
GET /api/jvm/history/{service}     – recent JVM snapshots (up to window_minutes)
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query, Request, status

from ..models.jvm import JvmMetricEvent

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/jvm", tags=["jvm"])


def _get_jvm_store(request: Request):
    store = getattr(request.app.state, "jvm_feature_store", None)
    if store is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="JVM feature store not initialised",
        )
    return store


@router.get(
    "/services",
    response_model=list[str],
    summary="List services with JVM metrics",
)
async def list_jvm_services(request: Request) -> list[str]:
    """Return names of all services that have at least one JVM snapshot."""
    store = _get_jvm_store(request)
    return store.get_services()


@router.get(
    "/health/{service_name}",
    response_model=JvmMetricEvent,
    summary="Latest JVM snapshot for a service",
)
async def get_jvm_health(service_name: str, request: Request) -> JvmMetricEvent:
    """Return the most recent JVM metric snapshot for *service_name*."""
    store = _get_jvm_store(request)
    snapshot = store.get_latest(service_name)
    if snapshot is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No JVM data for service '{service_name}'",
        )
    return snapshot


@router.get(
    "/history/{service_name}",
    response_model=list[JvmMetricEvent],
    summary="JVM snapshot history for a service",
)
async def get_jvm_history(
    service_name: str,
    request: Request,
    window_minutes: int = Query(
        default=60,
        ge=1,
        le=4320,
        description="Lookback window in minutes (1–4320)",
    ),
) -> list[JvmMetricEvent]:
    """Return JVM metric snapshots for *service_name* within the last N minutes."""
    store = _get_jvm_store(request)
    snapshots = store.get_snapshots(service_name, window_minutes=window_minutes)
    if not snapshots:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No JVM data for service '{service_name}' in the last {window_minutes} minutes",
        )
    return snapshots
