"""RED metrics history endpoints for dashboard overlay.

GET /api/metrics/services                      – list services with RED data
GET /api/metrics/series/{service}              – time-series for one metric
GET /api/metrics/snapshot/{service}            – latest RED snapshot (all metrics)
GET /api/metrics/dashboard                     – combined forecast + actual per service
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request, status
from pydantic import BaseModel

from ..models.metric import MetricPoint, REDMetric, TimeSeriesData

router = APIRouter(prefix="/api/metrics", tags=["metrics"])

_METRIC_NAMES = ("rate", "error_rate", "p50_ms", "p95_ms", "p99_ms")


def _get_feature_store(request: Request):
    store = getattr(request.app.state, "feature_store", None)
    if store is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Feature store not initialised",
        )
    return store


def _get_forecast_store(request: Request):
    return getattr(request.app.state, "forecast_store", None)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class ServiceInfo(BaseModel):
    service_name: str
    data_points: int
    metrics: List[str] = list(_METRIC_NAMES)


class DashboardEntry(BaseModel):
    service_name: str
    # Latest observed values
    current: Optional[REDMetric] = None
    # Forecast metadata (from ForecastStore)
    is_anomaly_predicted: bool = False
    anomaly_severity: Optional[str] = None
    forecast_model: Optional[str] = None
    forecast_generated_at: Optional[datetime] = None
    # How many observed data points are available
    data_points: int = 0


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get(
    "/services",
    response_model=List[ServiceInfo],
    summary="List services that have RED metric data",
)
async def list_services(request: Request) -> List[ServiceInfo]:
    """Return all services with at least one RED data point."""
    feature_store = _get_feature_store(request)
    services = feature_store.get_services()
    return [
        ServiceInfo(service_name=s, data_points=feature_store.size(s))
        for s in sorted(services)
    ]


@router.get(
    "/series/{service_name}",
    response_model=TimeSeriesData,
    summary="Historical time-series for a single RED metric",
)
async def get_metric_series(
    service_name: str,
    request: Request,
    metric: str = Query(
        default="p95_ms",
        description="Metric name: rate | error_rate | p50_ms | p95_ms | p99_ms",
    ),
    window_minutes: int = Query(
        default=60,
        ge=1,
        le=4320,
        description="Look-back window in minutes (1–4320, default 60)",
    ),
) -> TimeSeriesData:
    """Return observed time-series points for *metric* of *service_name*.

    Use this to overlay actual values with forecast predictions on a chart.
    """
    if metric not in _METRIC_NAMES:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unknown metric '{metric}'. Must be one of {_METRIC_NAMES}",
        )
    feature_store = _get_feature_store(request)
    points = feature_store.get_series(service_name, metric, window_minutes=window_minutes)
    if not points:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No data for service '{service_name}' metric '{metric}'",
        )
    return TimeSeriesData(service_name=service_name, metric_name=metric, points=points)


@router.get(
    "/snapshot/{service_name}",
    response_model=REDMetric,
    summary="Latest RED snapshot for a service (all 5 metrics)",
)
async def get_metric_snapshot(service_name: str, request: Request) -> REDMetric:
    """Return the most recent observed RED metric values for *service_name*.

    Useful for displaying current-state cards in a dashboard header.
    """
    feature_store = _get_feature_store(request)
    # Get the last 2 minutes to find the latest point
    points = feature_store.get_red_metrics(service_name, window_minutes=2)
    if not points:
        # Try the full store
        points = feature_store.get_red_metrics(service_name)
    if not points:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No data for service '{service_name}'",
        )
    return points[-1]


@router.get(
    "/dashboard",
    response_model=List[DashboardEntry],
    summary="Combined current metrics + forecast status per service",
)
async def get_dashboard_summary(request: Request) -> List[DashboardEntry]:
    """Return a dashboard-ready summary for every known service.

    Each entry includes:
    - Latest observed RED metric values
    - Anomaly prediction status and severity
    - Which forecast model was used and when it was generated
    - Number of available data points

    Poll this endpoint every 30–60 s to refresh the dashboard overview.
    """
    feature_store = _get_feature_store(request)
    forecast_store = _get_forecast_store(request)

    services = feature_store.get_services()
    entries: List[DashboardEntry] = []

    for service in sorted(services):
        # Latest observed snapshot
        current_points = feature_store.get_red_metrics(service, window_minutes=2)
        if not current_points:
            current_points = feature_store.get_red_metrics(service)
        current = current_points[-1] if current_points else None

        entry = DashboardEntry(
            service_name=service,
            current=current,
            data_points=feature_store.size(service),
        )

        # Enrich with forecast status for the primary metric (p95_ms)
        if forecast_store is not None:
            result = await forecast_store.get(service, "p95_ms")
            if result:
                entry.is_anomaly_predicted = result.is_anomaly_predicted
                entry.anomaly_severity = result.anomaly_severity
                entry.forecast_model = result.model_used
                entry.forecast_generated_at = result.generated_at

        entries.append(entry)

    return entries
