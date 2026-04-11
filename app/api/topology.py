"""Span topology API.

GET /api/topology/spans          – full service call graph from span parent links
GET /api/topology/spans/{service} – edges for one service (callers + callees)
GET /api/topology/spans/stats    – index / edge counts
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel

router = APIRouter(prefix="/api/topology", tags=["topology"])


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class TopologyEdgeResponse(BaseModel):
    caller: str
    callee: str
    call_count: int
    error_count: int
    error_rate: float
    last_seen: datetime


class TopologyResponse(BaseModel):
    edge_count: int
    edges: List[TopologyEdgeResponse]


class TopologyStatsResponse(BaseModel):
    span_index_size: int
    edge_count: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_tracker(request: Request):
    tracker = getattr(request.app.state, "span_topology", None)
    if tracker is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="SpanTopologyTracker is not initialised",
        )
    return tracker


def _edge_to_response(edge) -> TopologyEdgeResponse:
    error_rate = edge.error_count / edge.call_count if edge.call_count else 0.0
    return TopologyEdgeResponse(
        caller=edge.caller,
        callee=edge.callee,
        call_count=edge.call_count,
        error_count=edge.error_count,
        error_rate=round(error_rate, 6),
        last_seen=edge.last_seen,
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get(
    "/spans",
    response_model=TopologyResponse,
    summary="Full service call graph from real span parent_span_id links",
)
async def get_full_topology(request: Request) -> TopologyResponse:
    """Return all observed service-to-service call edges.

    Edges are derived from OTel ``parent_span_id`` relationships in real time
    and expire after 1 hour of inactivity.  Complements the Granger-based
    dependency map with actual runtime call topology.
    """
    tracker = _get_tracker(request)
    edges = tracker.get_topology()
    return TopologyResponse(
        edge_count=len(edges),
        edges=[_edge_to_response(e) for e in edges],
    )


@router.get(
    "/spans/stats",
    response_model=TopologyStatsResponse,
    summary="SpanTopologyTracker internal stats",
)
async def get_topology_stats(request: Request) -> TopologyStatsResponse:
    """Return diagnostic counters for the span topology tracker."""
    tracker = _get_tracker(request)
    return TopologyStatsResponse(
        span_index_size=tracker.index_size(),
        edge_count=tracker.edge_count(),
    )


@router.get(
    "/spans/{service_name}",
    response_model=TopologyResponse,
    summary="Call edges for a single service (callers + callees)",
)
async def get_service_topology(
    service_name: str, request: Request
) -> TopologyResponse:
    """Return all edges where *service_name* is either caller or callee."""
    tracker = _get_tracker(request)
    edges = tracker.get_callers(service_name) + tracker.get_callees(service_name)
    # Deduplicate (a service could appear in both lists if it calls itself)
    seen = set()
    unique: list = []
    for e in edges:
        key = (e.caller, e.callee)
        if key not in seen:
            seen.add(key)
            unique.append(e)
    if not unique:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No topology edges found for service '{service_name}'",
        )
    return TopologyResponse(
        edge_count=len(unique),
        edges=[_edge_to_response(e) for e in unique],
    )
