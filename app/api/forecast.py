"""Forecast query endpoints.

GET /api/forecast/red                     – all services, all metrics
GET /api/forecast/service/{service_name}  – specific service
GET /api/forecast/capacity                – capacity planning
GET /api/forecast/anomalies               – services with predicted anomalies
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request, status

from ..models.forecast import (
    CapacityForecast,
    ForecastRequest,
    ForecastResult,
    ModelType,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/forecast", tags=["forecast"])

_METRIC_NAMES = ("rate", "error_rate", "p50_ms", "p95_ms", "p99_ms")


def _get_forecast_store(request: Request):
    store = getattr(request.app.state, "forecast_store", None)
    if store is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Forecast store not initialised",
        )
    return store


def _get_feature_store(request: Request):
    store = getattr(request.app.state, "feature_store", None)
    if store is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Feature store not initialised",
        )
    return store


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get(
    "/red",
    response_model=List[ForecastResult],
    summary="Get latest forecasts for all services and metrics",
)
async def get_all_forecasts(request: Request) -> List[ForecastResult]:
    """Return the latest non-expired forecast for every (service, metric) pair."""
    forecast_store = _get_forecast_store(request)
    results = await forecast_store.list_all()
    return results


@router.get(
    "/service/{service_name}",
    response_model=List[ForecastResult],
    summary="Get forecast for a specific service",
)
async def get_service_forecast(
    service_name: str,
    request: Request,
    metric: str = Query(
        default="p95_ms",
        description="Metric to forecast (rate|error_rate|p50_ms|p95_ms|p99_ms). "
        "Pass 'all' to get all metrics.",
    ),
) -> List[ForecastResult]:
    """Return forecast(s) for *service_name*.

    * ``metric=p95_ms`` (default) – single metric
    * ``metric=all`` – all five RED metrics
    """
    forecast_store = _get_forecast_store(request)

    target_metrics = list(_METRIC_NAMES) if metric == "all" else [metric]
    if metric != "all" and metric not in _METRIC_NAMES:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unknown metric '{metric}'. Must be one of {_METRIC_NAMES} or 'all'",
        )

    results: List[ForecastResult] = []
    for m in target_metrics:
        result = await forecast_store.get(service_name, m)
        if result:
            results.append(result)

    if not results:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No forecast available for service '{service_name}' metric '{metric}'",
        )
    return results


@router.get(
    "/capacity",
    response_model=List[CapacityForecast],
    summary="Capacity planning forecast for all services",
)
async def get_capacity_forecast(request: Request) -> List[CapacityForecast]:
    """Return capacity-planning forecasts showing predicted peak rates and
    saturation risk per service."""
    forecast_store = _get_forecast_store(request)
    feature_store = _get_feature_store(request)

    services = feature_store.get_services()
    capacity_forecasts: List[CapacityForecast] = []

    for service in services:
        rate_forecast = await forecast_store.get(service, "rate")
        if rate_forecast is None or not rate_forecast.predictions:
            continue

        # Current rate: latest value in feature store
        rate_points = feature_store.get_series(service, "rate", window_minutes=5)
        current_rate = rate_points[-1].value if rate_points else 0.0

        # Peak predicted rate
        predicted_rates = [p.predicted for p in rate_forecast.predictions]
        peak_rate = max(predicted_rates)
        peak_idx = predicted_rates.index(peak_rate)
        peak_time = rate_forecast.predictions[peak_idx].timestamp

        # Saturation risk based on rate increase
        if current_rate > 0:
            growth_ratio = peak_rate / current_rate
        else:
            growth_ratio = 1.0

        if growth_ratio >= 4.0:
            risk = "critical"
            recommendation = (
                "Immediate scaling required – predicted 4x+ load increase"
            )
        elif growth_ratio >= 2.0:
            risk = "high"
            recommendation = (
                "Scale out within the forecast window – 2x+ load increase"
            )
        elif growth_ratio >= 1.5:
            risk = "medium"
            recommendation = (
                "Monitor closely and prepare to scale – 50%+ load increase"
            )
        else:
            risk = "low"
            recommendation = "No immediate action required"

        capacity_forecasts.append(
            CapacityForecast(
                service_name=service,
                current_rate=current_rate,
                predicted_peak_rate=peak_rate,
                predicted_peak_time=peak_time,
                saturation_risk=risk,
                recommendation=recommendation,
            )
        )

    return capacity_forecasts


@router.get(
    "/anomalies",
    response_model=List[ForecastResult],
    summary="Services with predicted anomalies",
)
async def get_predicted_anomalies(
    request: Request,
    severity: Optional[str] = Query(
        default=None,
        description="Filter by severity: warn | critical",
    ),
) -> List[ForecastResult]:
    """Return forecast results where an anomaly is predicted.

    Optional *severity* query parameter filters to ``warn`` or
    ``critical`` results only.
    """
    forecast_store = _get_forecast_store(request)
    all_results = await forecast_store.list_all()

    anomalies = [r for r in all_results if r.is_anomaly_predicted]

    if severity:
        anomalies = [
            r
            for r in anomalies
            if r.anomaly_severity == severity
        ]

    # Sort by severity (critical first) then service name
    severity_order = {"critical": 0, "warn": 1, None: 2}
    anomalies.sort(
        key=lambda r: (severity_order.get(r.anomaly_severity, 2), r.service_name)
    )
    return anomalies


@router.post(
    "/run",
    response_model=Dict[str, Any],
    summary="Trigger an on-demand forecast cycle",
)
async def trigger_forecast(
    req: ForecastRequest,
    request: Request,
) -> Dict[str, Any]:
    """Trigger an immediate forecast for the specified service/metric.

    Useful for testing or on-demand refresh outside the scheduled cycle.
    """
    forecaster = getattr(request.app.state, "forecaster", None)
    if forecaster is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Forecaster not initialised",
        )
    feature_store = _get_feature_store(request)
    points = feature_store.get_series(
        req.service_name,
        req.metric_name,
        window_minutes=forecaster.window_minutes,
    )
    if len(points) < forecaster.min_data_points:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Not enough data for '{req.service_name}' metric '{req.metric_name}': "
                f"have {len(points)}, need {forecaster.min_data_points}"
            ),
        )
    await forecaster._forecast_service_metric(req.service_name, req.metric_name)
    forecast_store = _get_forecast_store(request)
    result = await forecast_store.get(req.service_name, req.metric_name)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Forecast was run but result not found in store",
        )
    return result.model_dump()


@router.get(
    "/accuracy",
    summary="Forecast accuracy (MAE/RMSE) for all tracked (service, metric) pairs",
)
async def get_all_accuracy(request: Request):
    tracker = getattr(request.app.state, "accuracy_tracker", None)
    if tracker is None:
        return []
    pairs = await tracker.list_services()
    results = []
    for svc, metric in pairs:
        acc = await tracker.get_accuracy(svc, metric)
        results.append({"service_name": svc, "metric_name": metric, **acc})
    return results


@router.get(
    "/accuracy/{service_name}/{metric_name}",
    summary="Forecast accuracy (MAE/RMSE) for a specific service+metric",
)
async def get_accuracy(service_name: str, metric_name: str, request: Request):
    tracker = getattr(request.app.state, "accuracy_tracker", None)
    if tracker is None:
        raise HTTPException(status_code=503, detail="Accuracy tracker not initialised")
    acc = await tracker.get_accuracy(service_name, metric_name)
    return {"service_name": service_name, "metric_name": metric_name, **acc}
