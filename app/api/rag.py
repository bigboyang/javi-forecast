"""RAG / Text-to-SQL API endpoints.

POST /api/rag/query          – natural language → ClickHouse SQL → result rows
GET  /api/rag/schema         – return the schema context used by the LLM

POST /api/rag/incident       – manually store an anomaly incident
GET  /api/rag/incidents      – list stored incidents (optional ?service= filter)
POST /api/rag/incident/analyze – find similar incidents + LLM RCA for an anomaly

POST /api/rag/logs/ingest    – manually ingest a log record into the log store
POST /api/rag/logs/search    – semantic search over logs + LLM summary
GET  /api/rag/logs/recent    – fetch recent log records (optional ?service=&minutes=)
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from ..config import settings
from ..rag.schema_context import build_system_prompt

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/rag", tags=["rag"])


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class QueryRequest(BaseModel):
    question: str = Field(
        ...,
        min_length=5,
        max_length=2000,
        examples=["Show me the top 5 services by error rate in the last hour"],
    )


class QueryResponse(BaseModel):
    question: str
    sql: str
    columns: List[str]
    rows: List[List[Any]]
    row_count: int


class IncidentCreateRequest(BaseModel):
    service_name: str = Field(..., min_length=1)
    metric_name: str = Field(..., min_length=1)
    severity: str = Field(..., pattern="^(warn|critical)$")
    predicted_value: float
    expected_value: float
    z_score: float


class IncidentResponse(BaseModel):
    id: str
    service_name: str
    metric_name: str
    severity: str
    predicted_value: float
    expected_value: float
    z_score: float
    timestamp: datetime
    description: str


class IncidentAnalyzeRequest(BaseModel):
    service_name: str = Field(..., min_length=1)
    metric_name: str = Field(..., min_length=1)
    severity: str = Field(..., pattern="^(warn|critical)$")
    predicted_value: float
    expected_value: float
    z_score: float
    top_k: int = Field(default=5, ge=1, le=20)


class RCAResponse(BaseModel):
    service_name: str
    metric_name: str
    severity: str
    z_score: float
    similar_incidents: List[str]
    rca: Dict[str, Any]


class LogIngestRequest(BaseModel):
    service_name: str = Field(..., min_length=1)
    severity: str = Field(..., pattern="^(TRACE|DEBUG|INFO|WARN|ERROR|FATAL)$")
    body: str = Field(..., min_length=1)
    timestamp_ms: Optional[int] = Field(default=None, description="Epoch milliseconds; defaults to now")
    trace_id: Optional[str] = None
    span_id: Optional[str] = None


class LogIngestResponse(BaseModel):
    id: str
    service_name: str
    severity: str
    body: str
    timestamp: datetime
    trace_id: Optional[str]


class LogSearchRequest(BaseModel):
    question: str = Field(
        ...,
        min_length=3,
        max_length=2000,
        examples=["Show me ERROR logs from payment-service in the last hour"],
    )
    service_name: Optional[str] = Field(default=None, description="Filter by service name")
    top_k: int = Field(default=10, ge=1, le=50)


class LogSearchResponse(BaseModel):
    question: str
    service_name: Optional[str]
    matched_count: int
    logs: List[str]
    analysis: Dict[str, Any]


class LogRecordResponse(BaseModel):
    id: str
    service_name: str
    severity: str
    body: str
    timestamp: datetime
    trace_id: Optional[str]


# ---------------------------------------------------------------------------
# Dependency helpers
# ---------------------------------------------------------------------------


def _require_rag_enabled() -> None:
    if not settings.RAG_ENABLED:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="RAG is disabled – set RAG_ENABLED=true and ANTHROPIC_API_KEY",
        )


def _require_incident_rag_enabled() -> None:
    _require_rag_enabled()
    if not settings.INCIDENT_RAG_ENABLED:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Incident RAG is disabled – set INCIDENT_RAG_ENABLED=true",
        )


def _require_log_rag_enabled() -> None:
    _require_rag_enabled()
    if not settings.LOG_RAG_ENABLED:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Log RAG is disabled – set LOG_RAG_ENABLED=true",
        )


def _get_clickhouse(request: Request):
    ch = getattr(request.app.state, "clickhouse", None)
    if ch is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="ClickHouse is not available",
        )
    return ch


def _get_engine(request: Request):
    """Lazily create and cache the TextToSQLEngine on app.state."""
    engine = getattr(request.app.state, "text_to_sql_engine", None)
    if engine is None:
        from ..rag.text_to_sql import TextToSQLEngine

        try:
            engine = TextToSQLEngine()
        except RuntimeError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=str(exc),
            ) from exc
        request.app.state.text_to_sql_engine = engine
    return engine


def _get_incident_store(request: Request):
    store = getattr(request.app.state, "incident_store", None)
    if store is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="IncidentStore is not initialised",
        )
    return store


def _get_incident_rag(request: Request):
    """Lazily create and cache the IncidentRAG on app.state."""
    engine = getattr(request.app.state, "incident_rag_engine", None)
    if engine is None:
        from ..rag.incident_rag import IncidentRAG

        store = _get_incident_store(request)
        try:
            engine = IncidentRAG(store)
        except RuntimeError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=str(exc),
            ) from exc
        request.app.state.incident_rag_engine = engine
    return engine


def _get_log_store(request: Request):
    store = getattr(request.app.state, "log_store", None)
    if store is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="LogStore is not initialised – set LOG_RAG_ENABLED=true",
        )
    return store


def _get_log_rag(request: Request):
    """Lazily create and cache the LogRAG on app.state."""
    engine = getattr(request.app.state, "log_rag_engine", None)
    if engine is None:
        from ..rag.log_rag import LogRAG

        store = _get_log_store(request)
        try:
            engine = LogRAG(store)
        except RuntimeError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=str(exc),
            ) from exc
        request.app.state.log_rag_engine = engine
    return engine


# ---------------------------------------------------------------------------
# Text-to-SQL endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/query",
    response_model=QueryResponse,
    summary="Natural-language query → ClickHouse SQL → result rows",
)
async def rag_query(body: QueryRequest, request: Request) -> QueryResponse:
    """Convert a natural-language question to ClickHouse SQL and execute it."""
    _require_rag_enabled()
    clickhouse = _get_clickhouse(request)
    engine = _get_engine(request)

    try:
        result = await engine.query(body.question, clickhouse)
    except TimeoutError:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail=f"ClickHouse query timed out after {settings.RAG_SQL_TIMEOUT_SECONDS}s",
        )
    except Exception as exc:
        logger.error("RAG query failed: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Query failed: {exc}",
        )

    return QueryResponse(**result)


@router.get(
    "/schema",
    summary="Return the ClickHouse schema context used by the LLM",
    response_model=Dict[str, str],
)
async def get_schema() -> Dict[str, str]:
    """Expose the schema prompt so callers can understand which tables/columns
    are available and how the LLM is instructed to generate SQL."""
    return {"schema_context": build_system_prompt(settings.RAG_MAX_ROWS)}


# ---------------------------------------------------------------------------
# Incident RAG endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/incident",
    response_model=IncidentResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Manually store an anomaly incident",
)
async def create_incident(
    body: IncidentCreateRequest, request: Request
) -> IncidentResponse:
    """Store an anomaly incident in the vector store.

    This is called automatically by the forecaster when an anomaly is detected,
    but can also be called manually to seed historical incidents.

    Requires `INCIDENT_RAG_ENABLED=true`.
    """
    _require_incident_rag_enabled()
    store = _get_incident_store(request)

    incident = store.add_incident(
        service_name=body.service_name,
        metric_name=body.metric_name,
        severity=body.severity,
        predicted_value=body.predicted_value,
        expected_value=body.expected_value,
        z_score=body.z_score,
    )
    return IncidentResponse(
        id=incident.id,
        service_name=incident.service_name,
        metric_name=incident.metric_name,
        severity=incident.severity,
        predicted_value=incident.predicted_value,
        expected_value=incident.expected_value,
        z_score=incident.z_score,
        timestamp=incident.timestamp,
        description=incident.description,
    )


@router.get(
    "/incidents",
    response_model=List[IncidentResponse],
    summary="List stored incidents",
)
async def list_incidents(
    request: Request,
    service: Optional[str] = Query(default=None, description="Filter by service name"),
) -> List[IncidentResponse]:
    """Return all stored incidents, optionally filtered by service name.

    Requires `INCIDENT_RAG_ENABLED=true`.
    """
    _require_incident_rag_enabled()
    store = _get_incident_store(request)

    incidents = store.get_all(service_name=service)
    return [
        IncidentResponse(
            id=i.id,
            service_name=i.service_name,
            metric_name=i.metric_name,
            severity=i.severity,
            predicted_value=i.predicted_value,
            expected_value=i.expected_value,
            z_score=i.z_score,
            timestamp=i.timestamp,
            description=i.description,
        )
        for i in incidents
    ]


@router.post(
    "/incident/analyze",
    response_model=RCAResponse,
    summary="Find similar incidents + LLM Root Cause Analysis",
)
async def analyze_incident(
    body: IncidentAnalyzeRequest, request: Request
) -> RCAResponse:
    """Search for similar past incidents and generate an AI-powered RCA.

    The LLM (Claude) receives the current anomaly details and similar
    historical incidents as context to produce a root cause analysis
    with remediation steps.

    Requires `INCIDENT_RAG_ENABLED=true` and a valid `ANTHROPIC_API_KEY`.
    """
    _require_incident_rag_enabled()
    engine = _get_incident_rag(request)

    try:
        result = await engine.analyze(
            service_name=body.service_name,
            metric_name=body.metric_name,
            severity=body.severity,
            predicted_value=body.predicted_value,
            expected_value=body.expected_value,
            z_score=body.z_score,
            top_k=body.top_k,
        )
    except Exception as exc:
        logger.error("Incident RAG analysis failed: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Analysis failed: {exc}",
        )

    return RCAResponse(**result)


# ---------------------------------------------------------------------------
# Log RAG endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/logs/ingest",
    response_model=LogIngestResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Manually ingest a log record into the log store",
)
async def ingest_log(body: LogIngestRequest, request: Request) -> LogIngestResponse:
    """Store a single log record in the vector store.

    This is called automatically by the Kafka consumer when LOG_RAG_ENABLED=true,
    but can also be called manually to seed historical logs.

    Requires `LOG_RAG_ENABLED=true`.
    """
    _require_log_rag_enabled()
    store = _get_log_store(request)

    from datetime import timezone as _tz

    ts = None
    if body.timestamp_ms is not None:
        from datetime import datetime as _dt
        ts = _dt.fromtimestamp(body.timestamp_ms / 1000.0, tz=_tz.utc)

    record = store.add_log(
        service_name=body.service_name,
        severity=body.severity,
        body=body.body,
        timestamp=ts,
        trace_id=body.trace_id,
        span_id=body.span_id,
    )
    return LogIngestResponse(
        id=record.id,
        service_name=record.service_name,
        severity=record.severity,
        body=record.body,
        timestamp=record.timestamp,
        trace_id=record.trace_id,
    )


@router.post(
    "/logs/search",
    response_model=LogSearchResponse,
    summary="Semantic search over logs + LLM summary",
)
async def search_logs(body: LogSearchRequest, request: Request) -> LogSearchResponse:
    """Search for log records matching the natural-language question and
    generate an AI-powered summary.

    Requires `LOG_RAG_ENABLED=true` and a valid `ANTHROPIC_API_KEY`.
    """
    _require_log_rag_enabled()
    engine = _get_log_rag(request)

    try:
        result = await engine.search_and_analyze(
            question=body.question,
            service_name=body.service_name,
            top_k=body.top_k,
        )
    except Exception as exc:
        logger.error("Log RAG search failed: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Log search failed: {exc}",
        )

    return LogSearchResponse(**result)


@router.get(
    "/logs/recent",
    response_model=List[LogRecordResponse],
    summary="Fetch recent log records",
)
async def get_recent_logs(
    request: Request,
    service: Optional[str] = Query(default=None, description="Filter by service name"),
    minutes: int = Query(default=60, ge=1, le=1440, description="Lookback window in minutes"),
    limit: int = Query(default=100, ge=1, le=1000, description="Max records to return"),
) -> List[LogRecordResponse]:
    """Return recent log records stored in the log vector store.

    Requires `LOG_RAG_ENABLED=true`.
    """
    _require_log_rag_enabled()
    store = _get_log_store(request)

    records = store.get_recent(service_name=service, since_minutes=minutes, limit=limit)
    return [
        LogRecordResponse(
            id=r.id,
            service_name=r.service_name,
            severity=r.severity,
            body=r.body,
            timestamp=r.timestamp,
            trace_id=r.trace_id,
        )
        for r in records
    ]
