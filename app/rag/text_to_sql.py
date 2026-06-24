"""Text-to-SQL engine using Anthropic Claude.

Flow
----
1. Build a system prompt containing the ClickHouse schema context.
2. Send the user's natural-language question to Claude.
3. Extract the raw SQL from the model response.
4. Execute the SQL against ClickHouse (with row-cap and timeout).
5. Return rows + the generated SQL for transparency.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from ..config import settings
from .schema_context import build_system_prompt

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL extraction helpers
# ---------------------------------------------------------------------------

_SQL_FENCE_RE = re.compile(
    r"```(?:sql)?\s*([\s\S]+?)```", re.IGNORECASE
)

# Only SELECT statements are allowed — block DDL/DML that could mutate data.
_WRITE_KEYWORDS_RE = re.compile(
    r"^\s*(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|REPLACE|MERGE|ATTACH|DETACH)\b",
    re.IGNORECASE,
)


def _extract_sql(text: str) -> str:
    """Pull the first SQL block from the LLM response."""
    m = _SQL_FENCE_RE.search(text)
    if m:
        return m.group(1).strip()
    # No code fence – treat entire response as SQL
    return text.strip()


def _validate_readonly_sql(sql: str) -> None:
    """Raise ValueError if *sql* contains write/DDL statements."""
    if _WRITE_KEYWORDS_RE.match(sql):
        raise ValueError(f"Only SELECT queries are permitted. Got: {sql[:80]!r}")


# ---------------------------------------------------------------------------
# Main engine
# ---------------------------------------------------------------------------


class TextToSQLEngine:
    """Converts natural language questions into ClickHouse SQL via Claude."""

    def __init__(self) -> None:
        if not settings.ANTHROPIC_API_KEY:
            raise RuntimeError(
                "ANTHROPIC_API_KEY is not set – Text-to-SQL requires an Anthropic API key"
            )
        try:
            import anthropic  # lazy import so missing dep gives a clear error
        except ImportError as exc:
            raise RuntimeError(
                "anthropic package not installed – run: pip install anthropic>=0.25.0"
            ) from exc

        self._client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)
        self._model = settings.LLM_MODEL
        self._max_rows = settings.RAG_MAX_ROWS
        logger.info("TextToSQLEngine ready model=%s max_rows=%d", self._model, self._max_rows)

    async def generate_sql(self, question: str) -> str:
        """Ask the LLM to produce a ClickHouse SQL query for *question*.

        Returns the raw SQL string (without surrounding markdown).
        """
        import asyncio

        system_prompt = build_system_prompt(self._max_rows)
        loop = asyncio.get_running_loop()

        def _call() -> str:
            response = self._client.messages.create(
                model=self._model,
                max_tokens=1024,
                system=system_prompt,
                messages=[{"role": "user", "content": question}],
            )
            return response.content[0].text

        raw = await loop.run_in_executor(None, _call)
        sql = _extract_sql(raw)
        logger.debug("Generated SQL: %s", sql)
        return sql

    async def query(
        self,
        question: str,
        clickhouse_store,
    ) -> dict[str, Any]:
        """End-to-end: natural language → SQL → execute → return result dict.

        Parameters
        ----------
        question:
            Natural language question from the user.
        clickhouse_store:
            An initialised ``ClickHouseStore`` instance.

        Returns
        -------
        dict with keys:
            question  – original question
            sql       – generated SQL
            columns   – list of column names
            rows      – list of row tuples (capped at RAG_MAX_ROWS)
            row_count – actual number of rows returned
        """
        sql = await self.generate_sql(question)
        _validate_readonly_sql(sql)

        import asyncio

        result = await asyncio.wait_for(
            clickhouse_store._query(sql),
            timeout=settings.RAG_SQL_TIMEOUT_SECONDS,
        )

        columns: list[str] = result.column_names
        rows: list[list[Any]] = [list(r) for r in result.result_rows[: self._max_rows]]

        return {
            "question": question,
            "sql": sql,
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
        }
