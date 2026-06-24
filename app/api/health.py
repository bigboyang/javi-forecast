"""Health check endpoints.

GET /healthz  – liveness probe  (always 200 if the process is alive)
GET /readyz   – readiness probe (200 when all critical subsystems are healthy)
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import APIRouter, Request, Response

logger = logging.getLogger(__name__)

router = APIRouter(tags=["health"])

# Module-level state — set once at startup, never mutated during normal operation.
# A single-process async app has no data race here; asyncio is single-threaded.
_clickhouse_store = None
_feature_store = None
_kafka_service = None
_forecaster = None
_anomaly_detector = None
_ready = False


def set_dependencies(
    clickhouse_store,
    feature_store,
    ready: bool = False,
    kafka_service=None,
    forecaster=None,
    anomaly_detector=None,
) -> None:
    global _clickhouse_store, _feature_store, _ready
    global _kafka_service, _forecaster, _anomaly_detector
    _clickhouse_store = clickhouse_store
    _feature_store = feature_store
    _kafka_service = kafka_service
    _forecaster = forecaster
    _anomaly_detector = anomaly_detector
    _ready = ready


def mark_ready(ready: bool = True) -> None:
    global _ready
    _ready = ready


@router.get("/healthz")
async def liveness() -> dict[str, str]:
    """Liveness probe – always returns 200 while the process is running."""
    return {"status": "ok"}


@router.get("/readyz")
async def readiness(request: Request, response: Response) -> dict[str, Any]:
    """Readiness probe – 200 only when the service is fully initialised.

    Reads subsystem references from request.app.state when available,
    falling back to module-level state set at startup.
    """
    checks: dict[str, str] = {}

    clickhouse = getattr(request.app.state, "clickhouse", _clickhouse_store)
    feature_store = getattr(request.app.state, "feature_store", _feature_store)
    kafka = getattr(request.app.state, "kafka", _kafka_service)
    forecaster = getattr(request.app.state, "forecaster", _forecaster)
    anomaly_detector = getattr(request.app.state, "anomaly_detector", _anomaly_detector)

    if clickhouse is not None:
        try:
            ok = await asyncio.wait_for(clickhouse.ping(), timeout=3.0)
            checks["clickhouse"] = "ok" if ok else "unreachable"
        except TimeoutError:
            checks["clickhouse"] = "timeout"
        except Exception as exc:
            checks["clickhouse"] = f"error: {exc}"
    else:
        checks["clickhouse"] = "disabled"

    if feature_store is not None:
        services = feature_store.get_services()
        checks["feature_store"] = f"ok ({len(services)} services)"
    else:
        checks["feature_store"] = "not initialised"

    if kafka is not None:
        running = getattr(kafka, "_running", False)
        checks["kafka"] = "ok" if running else "stopped"
    else:
        checks["kafka"] = "disabled"

    if forecaster is not None:
        task = getattr(forecaster, "_task", None)
        if task is not None and not task.done():
            checks["forecaster"] = "ok"
        else:
            checks["forecaster"] = "not running"
    else:
        checks["forecaster"] = "disabled"

    if anomaly_detector is not None:
        task = getattr(anomaly_detector, "_task", None)
        if task is not None and not task.done():
            checks["anomaly_detector"] = "ok"
        else:
            checks["anomaly_detector"] = "not running"
    else:
        checks["anomaly_detector"] = "disabled"

    is_ready = _ready and all(
        v.startswith("ok") or v == "disabled" for v in checks.values()
    )
    if not is_ready:
        response.status_code = 503

    return {
        "status": "ready" if is_ready else "not_ready",
        "checks": checks,
    }
