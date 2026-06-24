"""Ingest endpoints.

POST /v1/spans        – span batches from javi-collector
POST /v1/metrics      – OTel metric batches from javi-collector
POST /v1/metrics/jvm  – JVM metric snapshots from javi-agent JvmSnapshotSender
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request, status

from ..models.jvm import JvmMetricBatch
from ..models.metric import MetricEvent, MetricEventBatch
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
) -> dict[str, int]:
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
) -> dict[str, int]:
    """Accept a single span event (convenience alias)."""
    handler = _get_event_handler(request)
    await handler.handle(span)
    return {"accepted": 1}


@router.post(
    "/v1/metrics",
    status_code=status.HTTP_200_OK,
    summary="Ingest a batch of OTel metric events",
)
async def ingest_metrics(
    batch: MetricEventBatch,
    request: Request,
) -> dict[str, int]:
    """Accept a batch of OTel metric data-points from javi-collector.

    Called by javi-collector's ``MetricForecastConsumer`` which POSTs
    individual metric data-points from the ``metrics`` Kafka topic.

    Returns ``{"accepted": N}`` where N is the number of events
    successfully enqueued.
    """
    metric_handler = getattr(request.app.state, "metric_event_handler", None)
    if metric_handler is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Metric event handler not initialised",
        )
    accepted = 0
    for event in batch.metrics:
        try:
            await metric_handler.handle(event)
            accepted += 1
        except Exception as exc:
            logger.error(
                "Failed to handle metric service=%s metric=%s: %s",
                event.service_name,
                event.metric_name,
                exc,
            )
    return {"accepted": accepted}


@router.post(
    "/v1/metric",
    status_code=status.HTTP_200_OK,
    summary="Ingest a single OTel metric event",
    include_in_schema=False,  # convenience alias, not in docs
)
async def ingest_single_metric(
    event: MetricEvent,
    request: Request,
) -> dict[str, int]:
    """Accept a single metric event (convenience alias)."""
    metric_handler = getattr(request.app.state, "metric_event_handler", None)
    if metric_handler is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Metric event handler not initialised",
        )
    await metric_handler.handle(event)
    return {"accepted": 1}


@router.post(
    "/v1/metrics/jvm",
    status_code=status.HTTP_200_OK,
    summary="Ingest JVM metric snapshots from javi-agent",
)
async def ingest_jvm_metrics(
    batch: JvmMetricBatch,
    request: Request,
) -> dict[str, int]:
    """Accept a batch of JVM metric snapshots from JvmSnapshotSender.

    Each snapshot contains heap usage, GC deltas, thread counts, and
    CPU utilization.  Events are stored in the JvmFeatureStore for
    analysis by JvmAnalyzer (OOM prediction, GC storm detection).
    """
    jvm_store = getattr(request.app.state, "jvm_feature_store", None)
    if jvm_store is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="JVM feature store not initialised",
        )
    accepted = 0
    for event in batch.metrics:
        try:
            await jvm_store.update(event)
            accepted += 1
        except Exception as exc:
            logger.error(
                "Failed to ingest JVM metric service=%s: %s",
                event.service_name,
                exc,
            )
    return {"accepted": accepted}
