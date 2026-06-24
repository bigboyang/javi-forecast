"""Log RAG: semantic log search + LLM summarisation.

Flow
----
1. Embed the user's question and search LogStore for relevant log records.
2. Format matching logs as LLM context.
3. Ask Claude to summarise the logs and highlight actionable signals.
4. Return structured result with matched logs + summary.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any

from ..config import settings
from .log_store import LogRecord, LogStore

logger = logging.getLogger(__name__)


_LOG_SYSTEM_PROMPT = """\
You are an expert Site Reliability Engineer (SRE) and log analyst.

Given a user question and a set of relevant application log records from
an APM system, provide a concise analysis as JSON with this exact structure:

{
  "summary": "<1-3 sentence summary of what the logs reveal>",
  "key_events": ["<notable log entry 1>", "<notable log entry 2>"],
  "likely_cause": "<root cause hypothesis based on logs, or 'insufficient data'>",
  "severity": "<critical|high|medium|low|info>",
  "remediation_steps": ["<step 1>", "<step 2>"],
  "needs_investigation": <true|false>
}

Rules:
- Base your analysis strictly on the provided log records.
- Set "needs_investigation" to true if you see ERROR or FATAL entries or
  patterns suggesting cascading failures.
- Keep each field concise.
- Return ONLY the JSON object, no markdown fences or extra text.
"""


class LogRAG:
    """Generates AI-powered log analysis using semantic search + Claude."""

    def __init__(self, log_store: LogStore) -> None:
        if not settings.ANTHROPIC_API_KEY:
            raise RuntimeError(
                "ANTHROPIC_API_KEY is not set – LogRAG requires an Anthropic API key"
            )
        try:
            import anthropic  # noqa: F401
        except ImportError as exc:
            raise RuntimeError(
                "anthropic package not installed – run: pip install anthropic>=0.25.0"
            ) from exc

        import anthropic as _anthropic

        self._client = _anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)
        self._model = settings.LLM_MODEL
        self._store = log_store
        logger.info("LogRAG ready model=%s", self._model)

    async def search_and_analyze(
        self,
        question: str,
        service_name: str | None = None,
        top_k: int = 10,
    ) -> dict[str, Any]:
        """Search relevant logs and generate LLM summary.

        Returns
        -------
        dict with keys:
            question, service_name, matched_count,
            logs        – list of matched log document strings,
            analysis    – parsed analysis dict from LLM,
        """
        records = self._store.search(question, service_name=service_name, top_k=top_k)
        user_message = self._build_user_message(question, service_name, records)

        loop = asyncio.get_event_loop()

        def _call() -> str:
            response = self._client.messages.create(
                model=self._model,
                max_tokens=1024,
                system=_LOG_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_message}],
            )
            return response.content[0].text

        raw = await loop.run_in_executor(None, _call)
        analysis = self._parse_response(raw)

        return {
            "question": question,
            "service_name": service_name,
            "matched_count": len(records),
            "logs": [r.document for r in records],
            "analysis": analysis,
        }

    def _parse_response(self, raw: str) -> dict[str, Any]:
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass
        m = re.search(r"\{[\s\S]+\}", raw)
        if m:
            try:
                return json.loads(m.group(0))
            except json.JSONDecodeError:
                pass
        return {
            "summary": raw.strip(),
            "key_events": [],
            "likely_cause": "insufficient data",
            "severity": "info",
            "remediation_steps": [],
            "needs_investigation": False,
        }

    def _build_user_message(
        self,
        question: str,
        service_name: str | None,
        records: list[LogRecord],
    ) -> str:
        lines = [
            "## Question",
            question,
            "",
        ]
        if service_name:
            lines += ["## Service Filter", service_name, ""]

        lines += ["## Matched Log Records"]
        if records:
            for i, r in enumerate(records, 1):
                lines.append(f"{i}. {r.document}")
        else:
            lines.append("(No matching log records found)")
        return "\n".join(lines)
