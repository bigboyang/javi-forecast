"""Unit tests for RCAEngine and hypothesis builder."""
import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from app.engine.rca_engine import RCAEngine, _build_hypothesis


def _anomaly(atype="latency_p95_spike", curr=200.0, base=15.0, z=10.0):
    return {
        "id": "abc123",
        "service_name": "svc-a",
        "span_name": "GET /api",
        "anomaly_type": atype,
        "minute": datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc),
        "current_value": curr,
        "baseline_value": base,
        "z_score": z,
        "severity": "critical",
        "detected_at": datetime(2026, 5, 14, 12, 1, tzinfo=timezone.utc),
    }


def _make_ch(anomalies=None):
    ch = MagicMock()
    ch.query_anomalies_recent = AsyncMock(return_value=anomalies or [])
    ch.query_correlated_spans = AsyncMock(return_value=[])
    ch.query_topology_neighbors = AsyncMock(return_value=[])
    ch.query_nearby_deployments = AsyncMock(return_value=[])
    ch.insert_rca_reports = AsyncMock()
    return ch


@pytest.mark.asyncio
async def test_rca_skips_when_no_anomalies():
    ch = _make_ch(anomalies=[])
    engine = RCAEngine(ch)
    await engine._process()
    ch.insert_rca_reports.assert_not_called()


@pytest.mark.asyncio
async def test_rca_generates_report_for_latency_spike():
    ch = _make_ch(anomalies=[_anomaly("latency_p95_spike")])
    engine = RCAEngine(ch)
    await engine._process()
    ch.insert_rca_reports.assert_called_once()
    reports = ch.insert_rca_reports.call_args[0][0]
    assert len(reports) == 1
    r = reports[0]
    assert r["service_name"] == "svc-a"
    assert r["anomaly_type"] == "latency_p95_spike"
    assert "Latency Spike" in r["hypothesis"]


@pytest.mark.asyncio
async def test_rca_generates_report_for_error_rate_spike():
    ch = _make_ch(anomalies=[_anomaly("error_rate_spike", curr=0.5, base=0.01)])
    engine = RCAEngine(ch)
    await engine._process()
    reports = ch.insert_rca_reports.call_args[0][0]
    assert "Error Rate Spike" in reports[0]["hypothesis"]


@pytest.mark.asyncio
async def test_rca_includes_deployment_in_hypothesis():
    dep = {
        "id": "dep1", "service_name": "svc-a", "version": "1.2.3",
        "environment": "prod", "deployed_by": "ci",
        "description": "release",
        "deployed_at": datetime(2026, 5, 14, 11, 58, tzinfo=timezone.utc),
    }
    ch = _make_ch(anomalies=[_anomaly()])
    ch.query_nearby_deployments = AsyncMock(return_value=[dep])
    engine = RCAEngine(ch)
    await engine._process()
    reports = ch.insert_rca_reports.call_args[0][0]
    assert "배포 감지" in reports[0]["hypothesis"]


# ── _build_hypothesis unit tests ──────────────────────────────────────────

def test_hypothesis_traffic_drop():
    a = _anomaly("traffic_drop", curr=1.0, base=10.0)
    h = _build_hypothesis(a, [], [], [])
    assert "Traffic Drop" in h
    assert "10.0" in h


def test_hypothesis_multivariate():
    a = _anomaly("multivariate_anomaly", curr=0.70, base=0.65)
    h = _build_hypothesis(a, [], [], [])
    assert "IsolationForest" in h


def test_hypothesis_neighbor_high_error():
    a = _anomaly()
    neighbors = [{"service_name": "db-svc", "direction": "downstream",
                  "call_count": 100, "error_rate": 0.20}]
    h = _build_hypothesis(a, [], neighbors, [])
    assert "db-svc" in h
    assert "20.0%" in h


def test_hypothesis_neighbor_low_error_excluded():
    a = _anomaly()
    neighbors = [{"service_name": "cache", "direction": "downstream",
                  "call_count": 100, "error_rate": 0.01}]
    h = _build_hypothesis(a, [], neighbors, [])
    assert "cache" not in h


@pytest.mark.asyncio
async def test_rca_start_stop():
    ch = _make_ch()
    engine = RCAEngine(ch)
    await engine.start()
    assert engine._task is not None
    await asyncio.sleep(0.05)
    await engine.stop()
    assert engine._task.done()
