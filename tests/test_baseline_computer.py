"""Unit tests for BaselineComputer."""
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from app.engine.baseline_computer import BaselineComputer


class _FakeClient:
    def __init__(self):
        self.commands = []

    def command(self, sql: str):
        self.commands.append(sql)


class _FakeCH:
    def __init__(self):
        self._database = "apm"
        self._client = _FakeClient()


@pytest.mark.asyncio
async def test_baseline_computer_runs_sql():
    ch = _FakeCH()
    bc = BaselineComputer(clickhouse=ch, interval_hours=1)

    # Run _compute() directly (don't start the background loop)
    await bc._compute()

    assert len(ch._client.commands) == 1
    sql = ch._client.commands[0]
    assert "red_baseline" in sql
    assert "spans" in sql
    assert "quantile(0.95)" in sql


@pytest.mark.asyncio
async def test_baseline_computer_start_stop():
    ch = _FakeCH()
    bc = BaselineComputer(clickhouse=ch, interval_hours=100)  # very long interval

    await bc.start()
    assert bc._task is not None
    await asyncio.sleep(0.05)
    await bc.stop()
    assert bc._task.cancelled() or bc._task.done()
