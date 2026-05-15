"""Aggregates all API routers into a single ``api_router``."""

from fastapi import APIRouter

from .health import router as health_router
from .ingest import router as ingest_router
from .forecast import router as forecast_router
from .metrics import router as metrics_router
from .jvm import router as jvm_router
from .dependency import router as dependency_router
from .rag import router as rag_router
from .topology import router as topology_router
from .feedback import router as feedback_router
from .registry import router as registry_router
from .events import router as events_router

api_router = APIRouter()

api_router.include_router(health_router)
api_router.include_router(ingest_router)
api_router.include_router(forecast_router)
api_router.include_router(metrics_router)
api_router.include_router(jvm_router)
api_router.include_router(dependency_router)
api_router.include_router(rag_router)
api_router.include_router(topology_router)
api_router.include_router(feedback_router)
api_router.include_router(registry_router)
api_router.include_router(events_router)
