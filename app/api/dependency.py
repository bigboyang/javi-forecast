"""Service dependency map REST API.

Endpoints
---------
GET /dependency/graph              — all causal edges in the graph
GET /dependency/{service}/causes   — root-cause analysis for a service
"""
from __future__ import annotations

from datetime import datetime
from typing import List

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

router = APIRouter(prefix="/dependency", tags=["dependency"])


class EdgeResponse(BaseModel):
    source: str
    target: str
    p_value: float
    max_lag: int
    updated_at: datetime


class RootCauseResponse(BaseModel):
    service: str
    root_causes: List[str]
    upstream_edges: List[EdgeResponse]


@router.get("/graph", response_model=List[EdgeResponse], summary="All causal dependency edges")
async def get_dependency_graph(request: Request) -> List[EdgeResponse]:
    """Return all Granger-causality edges currently in the dependency map."""
    dep_map = getattr(request.app.state, "dependency_map", None)
    if dep_map is None:
        return []
    return [
        EdgeResponse(
            source=e.source,
            target=e.target,
            p_value=e.p_value,
            max_lag=e.max_lag,
            updated_at=e.updated_at,
        )
        for e in dep_map.get_all_edges()
    ]


@router.get(
    "/{service}/causes",
    response_model=RootCauseResponse,
    summary="Root-cause analysis for a service",
)
async def get_root_causes(service: str, request: Request) -> RootCauseResponse:
    """Return upstream root-cause services and direct causal edges for *service*.

    Uses BFS traversal of the Granger causality graph to identify services
    with no further upstream cause (likely root causes).
    """
    dep_map = getattr(request.app.state, "dependency_map", None)
    if dep_map is None:
        raise HTTPException(status_code=503, detail="Dependency map not available")

    upstream_edges = dep_map.get_causes(service)
    root_causes = dep_map.get_root_causes(service)

    return RootCauseResponse(
        service=service,
        root_causes=root_causes,
        upstream_edges=[
            EdgeResponse(
                source=e.source,
                target=e.target,
                p_value=e.p_value,
                max_lag=e.max_lag,
                updated_at=e.updated_at,
            )
            for e in upstream_edges
        ],
    )
