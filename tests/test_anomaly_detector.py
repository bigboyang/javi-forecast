"""Unit tests for AnomalyDetector."""
import asyncio
import math
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from app.engine.anomaly_detector import AnomalyDetector, _IForestState


def _make_ch(points=None, baselines=None):
    ch = MagicMock()
    ch.query_current_red = AsyncMock(return_value=points or [])
    ch.query_baseline_all = AsyncMock(return_value=baselines or [])
    ch.insert_anomalies = AsyncMock()
    return ch


def _make_point(svc="svc-a", span="GET /api", route="/api",
                p50=10.0, p95=20.0, p99=30.0,
                error_rate=0.01, req_count=120):
    return {
        "service_name": svc, "span_name": span, "http_route": route,
        "minute": datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc),
        "req_count": req_count, "error_rate": error_rate,
        "p50_ms": p50, "p95_ms": p95, "p99_ms": p99,
    }


def _make_baseline(svc="svc-a", span="GET /api", route="/api",
                   dow=4, hour=12,
                   p50=10.0, p95=15.0, p99=20.0,
                   error_rate=0.01, avg_rps=2.0, sample_count=500):
    return {
        "service_name": svc, "span_name": span, "http_route": route,
        "day_of_week": dow, "hour_of_day": hour,
        "p50_ms": p50, "p95_ms": p95, "p99_ms": p99,
        "error_rate": error_rate, "avg_rps": avg_rps, "sample_count": sample_count,
    }


@pytest.mark.asyncio
async def test_no_anomaly_when_normal():
    point = _make_point(p95=16.0, error_rate=0.01)  # just above baseline
    baseline = _make_baseline(p95=15.0)
    ch = _make_ch(points=[point], baselines=[baseline])
    detector = AnomalyDetector(ch)

    await detector._detect()
    ch.insert_anomalies.assert_not_called()


@pytest.mark.asyncio
async def test_latency_spike_detected():
    # p95 much higher than baseline → latency_p95_spike
    point = _make_point(p95=200.0, p50=10.0, p99=250.0)
    baseline = _make_baseline(p95=15.0, p50=10.0, p99=20.0)
    ch = _make_ch(points=[point], baselines=[baseline])
    detector = AnomalyDetector(ch)

    await detector._detect()
    ch.insert_anomalies.assert_called_once()
    rows = ch.insert_anomalies.call_args[0][0]
    types = [r["anomaly_type"] for r in rows]
    assert "latency_p95_spike" in types


@pytest.mark.asyncio
async def test_error_rate_spike_detected():
    # error_rate 50% vs baseline 1%
    point = _make_point(p95=15.0, error_rate=0.50)
    baseline = _make_baseline(p95=15.0, error_rate=0.01, sample_count=1000)
    ch = _make_ch(points=[point], baselines=[baseline])
    detector = AnomalyDetector(ch)

    await detector._detect()
    ch.insert_anomalies.assert_called_once()
    rows = ch.insert_anomalies.call_args[0][0]
    types = [r["anomaly_type"] for r in rows]
    assert "error_rate_spike" in types


@pytest.mark.asyncio
async def test_traffic_drop_detected():
    # rps = 120/60 = 2.0, baseline avg_rps=10 → ratio < 0.5
    point = _make_point(req_count=60, p95=15.0)
    baseline = _make_baseline(avg_rps=10.0)
    ch = _make_ch(points=[point], baselines=[baseline])
    detector = AnomalyDetector(ch)

    await detector._detect()
    ch.insert_anomalies.assert_called_once()
    rows = ch.insert_anomalies.call_args[0][0]
    types = [r["anomaly_type"] for r in rows]
    assert "traffic_drop" in types


@pytest.mark.asyncio
async def test_suppression_window_deduplicates():
    point = _make_point(p95=200.0, p50=10.0, p99=250.0)
    baseline = _make_baseline(p95=15.0)
    ch = _make_ch(points=[point], baselines=[baseline])
    detector = AnomalyDetector(ch)

    await detector._detect()
    assert ch.insert_anomalies.call_count == 1

    # Second call within suppression window → same anomaly suppressed
    ch.insert_anomalies.reset_mock()
    ch.query_current_red = AsyncMock(return_value=[point])
    ch.query_baseline_all = AsyncMock(return_value=[baseline])
    await detector._detect()
    ch.insert_anomalies.assert_not_called()


@pytest.mark.asyncio
async def test_missing_baseline_skipped():
    point = _make_point(svc="new-service")
    baseline = _make_baseline(svc="other-service")
    ch = _make_ch(points=[point], baselines=[baseline])
    detector = AnomalyDetector(ch)

    await detector._detect()
    ch.insert_anomalies.assert_not_called()
