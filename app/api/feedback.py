"""Alert feedback API.

POST /api/alerts/feedback        – submit false-positive / false-negative feedback
GET  /api/alerts/feedback        – list stored feedback entries
GET  /api/alerts/feedback/stats  – aggregate FP/FN counts per service
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api/alerts", tags=["feedback"])


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class FeedbackRequest(BaseModel):
    service_name: str = Field(..., min_length=1)
    metric_name: str = Field(..., min_length=1)
    severity: str = Field(..., pattern="^(warn|critical)$")
    is_false_positive: bool = Field(
        ...,
        description="True = alert fired but nothing was wrong (FP). "
        "False = alert did NOT fire but an issue existed (FN).",
    )
    note: str | None = Field(
        default=None,
        max_length=2000,
        description="Free-text operator note (root cause, ticket link, etc.)",
    )


class FeedbackResponse(BaseModel):
    id: str
    service_name: str
    metric_name: str
    severity: str
    is_false_positive: bool
    note: str | None
    created_at: datetime


class FeedbackStatsResponse(BaseModel):
    total: int
    false_positives_by_service: dict[str, int]
    false_negatives_by_service: dict[str, int]


# ---------------------------------------------------------------------------
# Dependency helper
# ---------------------------------------------------------------------------


def _get_store(request: Request):
    store = getattr(request.app.state, "feedback_store", None)
    if store is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="FeedbackStore is not initialised",
        )
    return store


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/feedback",
    response_model=FeedbackResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Submit false-positive or false-negative feedback for an alert",
)
async def submit_feedback(
    body: FeedbackRequest, request: Request
) -> FeedbackResponse:
    """Record operator feedback to improve alert accuracy over time.

    - **is_false_positive = true**: The alert fired but there was no real issue.
    - **is_false_positive = false**: A real issue occurred that the system missed
      (false negative).

    Feedback is stored in-memory (TTL 7 days) and surfaced in
    ``GET /api/alerts/feedback/stats`` for model calibration dashboards.
    """
    store = _get_store(request)
    entry = store.add(
        service_name=body.service_name,
        metric_name=body.metric_name,
        severity=body.severity,
        is_false_positive=body.is_false_positive,
        note=body.note,
    )
    return FeedbackResponse(
        id=entry.id,
        service_name=entry.service_name,
        metric_name=entry.metric_name,
        severity=entry.severity,
        is_false_positive=entry.is_false_positive,
        note=entry.note,
        created_at=entry.created_at,
    )


@router.get(
    "/feedback",
    response_model=list[FeedbackResponse],
    summary="List stored alert feedback entries",
)
async def list_feedback(
    request: Request,
    service: str | None = Query(default=None, description="Filter by service name"),
    metric: str | None = Query(default=None, description="Filter by metric name"),
) -> list[FeedbackResponse]:
    """Return operator feedback entries, newest first."""
    store = _get_store(request)
    entries = store.get_all(service_name=service, metric_name=metric)
    return [
        FeedbackResponse(
            id=e.id,
            service_name=e.service_name,
            metric_name=e.metric_name,
            severity=e.severity,
            is_false_positive=e.is_false_positive,
            note=e.note,
            created_at=e.created_at,
        )
        for e in entries
    ]


@router.get(
    "/feedback/stats",
    response_model=FeedbackStatsResponse,
    summary="Aggregate false-positive / false-negative counts per service",
)
async def get_feedback_stats(request: Request) -> FeedbackStatsResponse:
    """Return a summary of FP/FN feedback counts per service.

    Use this to identify the noisiest alert sources and prioritise
    threshold tuning or model recalibration.
    """
    store = _get_store(request)
    raw = store.stats()
    return FeedbackStatsResponse(
        total=raw["total"],
        false_positives_by_service=raw["false_positives_by_service"],
        false_negatives_by_service=raw["false_negatives_by_service"],
    )
