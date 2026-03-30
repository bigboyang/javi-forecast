"""Prometheus metrics exposition endpoint.

GET /metrics – exposes all registered prometheus_client metrics in the
standard text-based format consumed by Prometheus scrapers.

All application metrics are registered lazily by the modules that own
them (forecaster, event_handler, etc.).  This module only exposes the
collection endpoint.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    generate_latest,
    REGISTRY,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["metrics"])


@router.get("/metrics")
async def metrics() -> Response:
    """Return Prometheus text-format metrics."""
    output = generate_latest(REGISTRY)
    return Response(
        content=output,
        media_type=CONTENT_TYPE_LATEST,
    )
