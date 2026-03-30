"""Span ingest endpoint.

POST /v1/spans – receives span batches from javi-collector's
ForecastConsumer and feeds them to the EventHandler for feature-store
ingestion.
"""

from __future__ import annotations

import logging
from typing import Dict

from fastapi import APIRouter, HTTPException, Request, status

from ..models.span import SpanBatch, SpanEvent

logger = logging.getLogger(__name__)

router = APIRouter(tags=["ingest"])


def _get_event_handler(request: Request):
    """Retrieve EventHandler from app state (injected at startup)."""
    handler = getattr(request.app.state, "event_handler", None)
    if handler is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Event handler not initialised",
        )
    return handler


@router.post(
    "/v1/spans",
    status_code=status.HTTP_200_OK,
    summary="Ingest a batch of span events",
)
async def ingest_spans(
    batch: SpanBatch,
    request: Request,
) -> Dict[str, int]:
    """Accept a batch of OTel span events from javi-collector.

    Called by javi-collector's ``ForecastConsumer`` which POSTs
    error/warn/slow spans destined for anomaly forecasting.

    Returns ``{"accepted": N}`` where N is the number of spans
    successfully enqueued.
    """
    handler = _get_event_handler(request)
    accepted = 0
    for span in batch.spans:
        try:
            await handler.handle(span)
            accepted += 1
        except Exception as exc:
            logger.error(
                "Failed to handle span trace_id=%s: %s",
                span.trace_id,
                exc,
            )
    return {"accepted": accepted}


@router.post(
    "/v1/span",
    status_code=status.HTTP_200_OK,
    summary="Ingest a single span event",
    include_in_schema=False,   # convenience alias, not in docs
)
async def ingest_single_span(
    span: SpanEvent,
    request: Request,
) -> Dict[str, int]:
    """Accept a single span event (convenience alias)."""
    handler = _get_event_handler(request)
    await handler.handle(span)
    return {"accepted": 1}
