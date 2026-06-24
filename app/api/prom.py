"""Prometheus metrics scrape endpoint.

GET /metrics  – returns all registered metrics in Prometheus text format.

This is intentionally a plain text endpoint (not JSON) consumed by a
Prometheus scraper, not a human browser.  It is excluded from the OpenAPI
docs to avoid confusion with the RED-metrics JSON endpoints under /api/metrics.
"""
from fastapi import APIRouter
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

router = APIRouter(tags=["observability"])


@router.get("/metrics", include_in_schema=False)
async def prometheus_metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
