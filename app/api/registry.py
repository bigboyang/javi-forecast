"""Service registry API.

GET /api/services           – list all services with OTel Resource metadata
GET /api/services/{service} – resource metadata for a single service
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel

router = APIRouter(prefix="/api/services", tags=["registry"])


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class ServiceMetadataResponse(BaseModel):
    service_name: str
    service_version: Optional[str]
    deployment_environment: Optional[str]
    k8s_pod_name: Optional[str]
    k8s_namespace: Optional[str]
    cloud_region: Optional[str]
    host_name: Optional[str]
    extra: Dict[str, Any]
    last_seen: datetime


class ServiceListResponse(BaseModel):
    total: int
    services: List[ServiceMetadataResponse]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_registry(request: Request):
    registry = getattr(request.app.state, "service_registry", None)
    if registry is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="ServiceRegistry is not initialised",
        )
    return registry


def _meta_to_response(meta) -> ServiceMetadataResponse:
    return ServiceMetadataResponse(
        service_name=meta.service_name,
        service_version=meta.service_version,
        deployment_environment=meta.deployment_environment,
        k8s_pod_name=meta.k8s_pod_name,
        k8s_namespace=meta.k8s_namespace,
        cloud_region=meta.cloud_region,
        host_name=meta.host_name,
        extra=meta.extra,
        last_seen=meta.last_seen,
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get(
    "",
    response_model=ServiceListResponse,
    summary="List all services with their OTel Resource metadata",
)
async def list_services(request: Request) -> ServiceListResponse:
    """Return all services whose spans have carried OTel Resource attributes.

    Attributes are updated in real-time as new spans arrive, so a
    rolling deployment will show the latest ``service.version`` as pods
    are replaced.
    """
    registry = _get_registry(request)
    services = registry.get_all()
    return ServiceListResponse(
        total=len(services),
        services=[_meta_to_response(m) for m in services],
    )


@router.get(
    "/{service_name}",
    response_model=ServiceMetadataResponse,
    summary="OTel Resource metadata for a single service",
)
async def get_service_metadata(
    service_name: str, request: Request
) -> ServiceMetadataResponse:
    """Return the latest OTel Resource attributes observed for *service_name*."""
    registry = _get_registry(request)
    meta = registry.get(service_name)
    if meta is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No resource metadata found for service '{service_name}'",
        )
    return _meta_to_response(meta)
