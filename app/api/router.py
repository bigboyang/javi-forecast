"""Aggregates all API routers into a single ``api_router``."""

from fastapi import APIRouter

from .health import router as health_router
from .ingest import router as ingest_router
from .forecast import router as forecast_router
from .prometheus import router as prometheus_router
from .jvm import router as jvm_router
from .dependency import router as dependency_router

api_router = APIRouter()

api_router.include_router(health_router)
api_router.include_router(ingest_router)
api_router.include_router(forecast_router)
api_router.include_router(prometheus_router)
api_router.include_router(jvm_router)
api_router.include_router(dependency_router)
