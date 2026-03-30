"""Health check endpoints.

GET /healthz  – liveness probe  (always 200 if the process is alive)
GET /readyz   – readiness probe (200 when ClickHouse is reachable and
                the feature store has been initialised)
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from fastapi import APIRouter, Response

logger = logging.getLogger(__name__)

router = APIRouter(tags=["health"])

# Set by main.py during startup so health checks can inspect state
_clickhouse_store = None
_feature_store = None
_ready = False


def set_dependencies(clickhouse_store, feature_store, ready: bool = False) -> None:
    global _clickhouse_store, _feature_store, _ready
    _clickhouse_store = clickhouse_store
    _feature_store = feature_store
    _ready = ready


def mark_ready(ready: bool = True) -> None:
    global _ready
    _ready = ready


@router.get("/healthz")
async def liveness() -> Dict[str, str]:
    """Liveness probe – always returns 200 while the process is running."""
    return {"status": "ok"}


@router.get("/readyz")
async def readiness(response: Response) -> Dict[str, Any]:
    """Readiness probe – 200 only when the service is fully initialised."""
    checks: Dict[str, str] = {}

    # ClickHouse reachability
    if _clickhouse_store is not None:
        try:
            ok = await _clickhouse_store.ping()
            checks["clickhouse"] = "ok" if ok else "unreachable"
        except Exception as exc:
            checks["clickhouse"] = f"error: {exc}"
    else:
        checks["clickhouse"] = "disabled"

    # Feature store populated
    if _feature_store is not None:
        services = _feature_store.get_services()
        checks["feature_store"] = f"ok ({len(services)} services)"
    else:
        checks["feature_store"] = "not initialised"

    # Overall readiness
    is_ready = _ready and all(
        v.startswith("ok") or v == "disabled" for v in checks.values()
    )
    if not is_ready:
        response.status_code = 503

    return {
        "status": "ready" if is_ready else "not_ready",
        "checks": checks,
    }
