"""RAG / Text-to-SQL API endpoints.

POST /api/rag/query   – natural language → ClickHouse SQL → result rows
GET  /api/rag/schema  – return the schema context used by the LLM
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request, status
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


# ---------------------------------------------------------------------------
# Dependency helpers
# ---------------------------------------------------------------------------


def _require_rag_enabled() -> None:
    if not settings.RAG_ENABLED:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="RAG is disabled – set RAG_ENABLED=true and ANTHROPIC_API_KEY",
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


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/query",
    response_model=QueryResponse,
    summary="Natural-language query → ClickHouse SQL → result rows",
)
async def rag_query(body: QueryRequest, request: Request) -> QueryResponse:
    """Convert a natural-language question to ClickHouse SQL and execute it.

    The LLM (Claude) uses the APM schema context to generate safe, read-only
    SQL. The generated SQL and result rows are both returned so you can verify
    correctness.

    Requires `RAG_ENABLED=true` and a valid `ANTHROPIC_API_KEY`.
    """
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
