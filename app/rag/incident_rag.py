"""Incident RAG: find similar past incidents + LLM Root Cause Analysis.

Flow
----
1. Search IncidentStore for past incidents similar to the current anomaly.
2. Format similar incidents as LLM context.
3. Ask Claude to generate an RCA and remediation suggestions.
4. Return structured result with similar incidents + RCA.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any, Dict, List

from ..config import settings
from .incident_store import Incident, IncidentStore

logger = logging.getLogger(__name__)


_RCA_SYSTEM_PROMPT = """\
You are an expert Site Reliability Engineer (SRE) and APM analyst.

Given the current anomaly and similar past incidents from an APM system,
produce a concise Root Cause Analysis (RCA) as JSON with this exact structure:

{
  "likely_cause": "<short description of the most probable root cause>",
  "confidence": "<high|medium|low>",
  "evidence": ["<evidence item 1>", "<evidence item 2>"],
  "remediation_steps": ["<step 1>", "<step 2>"],
  "escalate": <true|false>
}

Rules:
- Base your analysis only on the provided context.
- Set "escalate" to true if severity is "critical" or z_score > 3.0.
- Keep each field concise.
- Return ONLY the JSON object, no markdown fences or extra text.
"""


class IncidentRAG:
    """Generates AI-powered RCA using past incident context."""

    def __init__(self, incident_store: IncidentStore) -> None:
        if not settings.ANTHROPIC_API_KEY:
            raise RuntimeError(
                "ANTHROPIC_API_KEY is not set – IncidentRAG requires an Anthropic API key"
            )
        try:
            import anthropic
        except ImportError as exc:
            raise RuntimeError(
                "anthropic package not installed – run: pip install anthropic>=0.25.0"
            ) from exc

        import anthropic as _anthropic

        self._client = _anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)
        self._model = settings.LLM_MODEL
        self._store = incident_store
        logger.info("IncidentRAG ready model=%s", self._model)

    async def analyze(
        self,
        service_name: str,
        metric_name: str,
        severity: str,
        predicted_value: float,
        expected_value: float,
        z_score: float,
        top_k: int = 5,
    ) -> Dict[str, Any]:
        """Find similar incidents and generate RCA.

        Returns
        -------
        dict with keys:
            service_name, metric_name, severity, z_score,
            similar_incidents  – list of past incident descriptions,
            rca                – parsed RCA dict from LLM,
            raw_rca            – raw LLM response text
        """
        similar = self._store.search_similar(service_name, metric_name, top_k=top_k)
        user_message = self._build_user_message(
            service_name, metric_name, severity,
            predicted_value, expected_value, z_score, similar,
        )

        loop = asyncio.get_event_loop()

        def _call() -> str:
            response = self._client.messages.create(
                model=self._model,
                max_tokens=1024,
                system=_RCA_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_message}],
            )
            return response.content[0].text

        raw_rca = await loop.run_in_executor(None, _call)
        rca = self._parse_rca(raw_rca)

        return {
            "service_name": service_name,
            "metric_name": metric_name,
            "severity": severity,
            "z_score": z_score,
            "similar_incidents": [i.description for i in similar],
            "rca": rca,
        }

    def _parse_rca(self, raw: str) -> Dict[str, Any]:
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
        # Fallback: wrap raw text
        return {
            "likely_cause": raw.strip(),
            "confidence": "low",
            "evidence": [],
            "remediation_steps": [],
            "escalate": False,
        }

    def _build_user_message(
        self,
        service_name: str,
        metric_name: str,
        severity: str,
        predicted_value: float,
        expected_value: float,
        z_score: float,
        similar: List[Incident],
    ) -> str:
        lines = [
            "## Current Anomaly",
            f"- Service: {service_name}",
            f"- Metric: {metric_name}",
            f"- Severity: {severity}",
            f"- Predicted value: {predicted_value:.4f}",
            f"- Expected value: {expected_value:.4f}",
            f"- Z-score: {z_score:.2f}",
            "",
            "## Similar Past Incidents",
        ]
        if similar:
            for i, inc in enumerate(similar, 1):
                lines.append(f"{i}. {inc.description}")
        else:
            lines.append("(No similar past incidents found)")
        return "\n".join(lines)
