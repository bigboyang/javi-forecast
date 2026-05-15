"""RCA Engine (ported from javi-collector Go rca pkg).


Processing pipeline (every RCA_INTERVAL_SECONDS):
  1. Query anomalies table for records in the last 2× interval window.
  2. For each anomaly: fetch correlated ERROR spans, topology neighbors,
     nearby deployment events.
  3. Build a rule-based hypothesis string.
  4. Batch-INSERT rca_reports into ClickHouse.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone, timedelta

from ..config import settings


def _to_json(obj) -> str:
    """JSON-serialize *obj*, converting datetime to ISO strings."""
    def _default(o):
        if isinstance(o, datetime):
            return o.isoformat()
        raise TypeError(f"Object of type {type(o)} is not JSON serializable")
    return json.dumps(obj, default=_default)

logger = logging.getLogger(__name__)


class RCAEngine:
    """Background RCA processor.

    Parameters
    ----------
    clickhouse:
        Connected ClickHouseStore instance.
    """

    def __init__(self, clickhouse) -> None:
        self._ch = clickhouse
        self._task = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="rca-engine")
        logger.info("RCAEngine started interval=%ds", settings.RCA_INTERVAL_SECONDS)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("RCAEngine stopped")

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(settings.RCA_INTERVAL_SECONDS)
            try:
                await self._process()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("rca: process cycle error: %s", exc)

    async def _process(self) -> None:
        window = settings.RCA_INTERVAL_SECONDS * 2
        try:
            anomalies = await self._ch.query_anomalies_recent(window)
        except Exception as exc:
            logger.error("rca: fetch anomalies failed: %s", exc)
            return

        if not anomalies:
            return

        reports: list[dict] = []
        for a in anomalies:
            try:
                report = await self._analyze(a)
                reports.append(report)
            except Exception as exc:
                logger.warning("rca: analyze failed anomaly_id=%s: %s", a["id"], exc)

        if not reports:
            return

        try:
            await self._ch.insert_rca_reports(reports)
            logger.info("rca: reports generated count=%d", len(reports))
        except Exception as exc:
            logger.error("rca: insert failed: %s", exc)

    async def _analyze(self, a: dict) -> dict:
        minute = a["minute"]
        from_ts = minute - timedelta(minutes=30)
        to_ts = minute + timedelta(minutes=30)

        spans, neighbors, deployments = await asyncio.gather(
            self._fetch_correlated_spans(a["service_name"], minute),
            self._fetch_topology_neighbors(a["service_name"], from_ts, to_ts),
            self._fetch_nearby_deployments(a["service_name"], minute),
            return_exceptions=True,
        )

        spans = spans if isinstance(spans, list) else []
        neighbors = neighbors if isinstance(neighbors, list) else []
        deployments = deployments if isinstance(deployments, list) else []

        hypothesis = _build_hypothesis(a, spans, neighbors, deployments)

        return {
            "id": uuid.uuid4().hex,
            "anomaly_id": a["id"],
            "service_name": a["service_name"],
            "span_name": a["span_name"],
            "anomaly_type": a["anomaly_type"],
            "minute": a["minute"],
            "severity": a["severity"],
            "z_score": a["z_score"],
            "correlated_spans": _to_json(spans),
            "similar_incidents": "[]",
            "nearby_deployments": _to_json(deployments),
            "hypothesis": hypothesis,
            "llm_analysis": "",
            "created_at": datetime.now(tz=timezone.utc),
        }

    async def _fetch_correlated_spans(self, svc: str, minute: datetime) -> list:
        try:
            return await self._ch.query_correlated_spans(svc, minute)
        except Exception as exc:
            logger.warning("rca: correlated spans failed svc=%s: %s", svc, exc)
            return []

    async def _fetch_topology_neighbors(
        self, svc: str, from_ts: datetime, to_ts: datetime
    ) -> list:
        try:
            neighbors = await self._ch.query_topology_neighbors(svc, from_ts, to_ts)
            for n in neighbors:
                n["hop"] = 1
            return neighbors
        except Exception as exc:
            logger.warning("rca: topology fetch failed svc=%s: %s", svc, exc)
            return []

    async def _fetch_nearby_deployments(self, svc: str, minute: datetime) -> list:
        try:
            return await self._ch.query_nearby_deployments(svc, minute)
        except Exception as exc:
            logger.warning("rca: deployment events failed svc=%s: %s", svc, exc)
            return []


def _build_hypothesis(
    a: dict,
    spans: list,
    neighbors: list,
    deployments: list,
) -> str:
    parts: list[str] = []

    atype = a["anomaly_type"]
    svc = a["service_name"]
    span = a["span_name"]
    curr = a["current_value"]
    base = a["baseline_value"]
    z = a["z_score"]

    if atype == "latency_p95_spike":
        parts.append(
            f"[Latency Spike] {svc}/{span} P95 응답 시간이 기준값({base:.1f}ms) 대비 "
            f"{curr:.1f}ms(Z={z:.1f})으로 급증."
        )
    elif atype == "error_rate_spike":
        parts.append(
            f"[Error Rate Spike] {svc}/{span} 에러율이 기준값({base*100:.1f}%) 대비 "
            f"{curr*100:.1f}%(Z={z:.1f})으로 급증."
        )
    elif atype == "traffic_drop":
        ratio = (curr / base * 100) if base > 0 else 0
        parts.append(
            f"[Traffic Drop] {svc}/{span} 요청량이 기준값({base:.1f} RPS) 대비 {ratio:.0f}%로 감소."
        )
    elif atype == "multivariate_anomaly":
        parts.append(
            f"[Multivariate Anomaly] {svc}/{span} IsolationForest score={curr:.2f} "
            f"(threshold={base:.2f})."
        )
    else:
        parts.append(f"[{atype}] {svc}/{span} 이상 감지.")

    if spans:
        top = spans[0]
        exc = f", {top['exception_type']}" if top.get("exception_type") else ""
        parts.append(f"가장 긴 에러 스팬: {top['name']} ({top['duration_ms']:.0f}ms{exc}).")

    for n in neighbors:
        if n.get("error_rate", 0) >= 0.05:
            parts.append(
                f"{n['service_name']} 서비스({n['direction']}) 에러율 "
                f"{n['error_rate']*100:.1f}% 감지."
            )

    for d in deployments:
        deployed_at = d.get("deployed_at")
        if deployed_at:
            minute = a["minute"]
            if isinstance(deployed_at, datetime) and isinstance(minute, datetime):
                diff = (deployed_at - minute).total_seconds() / 60
                direction = "후" if diff > 0 else "전"
                parts.append(
                    f"[배포 감지] {d['service_name']} v{d['version']} "
                    f"({d['environment']}, by {d['deployed_by']}) — "
                    f"이상 발생 {abs(diff):.0f}분 {direction}."
                )

    return " ".join(parts)
