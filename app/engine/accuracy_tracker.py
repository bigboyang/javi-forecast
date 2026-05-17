"""Forecast prediction accuracy tracker.

Records (service, metric, predicted_for_ts, predicted_value) tuples at
prediction time.  When actual observations arrive, matches them by timestamp
(±90 s tolerance) and computes rolling MAE / RMSE over the N most recent
evaluated predictions.
"""
from __future__ import annotations

import asyncio
import math
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Deque, Dict, List, Optional, Tuple


@dataclass
class _PredRecord:
    service_name: str
    metric_name: str
    predicted_for: datetime
    predicted_value: float
    actual_value: Optional[float] = None
    error: Optional[float] = None  # actual - predicted


class AccuracyTracker:
    """Tracks forecast vs actual for rolling accuracy computation.

    Parameters
    ----------
    maxlen:
        Maximum number of prediction records per (service, metric) pair.
    match_window_seconds:
        Tolerance (in seconds) when matching a predicted timestamp to an
        observed timestamp.
    """

    def __init__(
        self,
        maxlen: int = 500,
        match_window_seconds: int = 90,
    ) -> None:
        self._maxlen = maxlen
        self._match_window = timedelta(seconds=match_window_seconds)
        self._preds: Dict[Tuple[str, str], Deque[_PredRecord]] = {}
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Write side
    # ------------------------------------------------------------------

    async def record_prediction(
        self,
        service_name: str,
        metric_name: str,
        predicted_for: datetime,
        predicted_value: float,
    ) -> None:
        key = (service_name, metric_name)
        rec = _PredRecord(
            service_name=service_name,
            metric_name=metric_name,
            predicted_for=predicted_for,
            predicted_value=predicted_value,
        )
        async with self._lock:
            if key not in self._preds:
                self._preds[key] = deque(maxlen=self._maxlen)
            self._preds[key].append(rec)

    async def record_actual(
        self,
        service_name: str,
        metric_name: str,
        ts: datetime,
        actual_value: float,
    ) -> None:
        key = (service_name, metric_name)
        async with self._lock:
            preds = self._preds.get(key)
            if preds is None:
                return
            for rec in preds:
                if rec.actual_value is None:
                    diff = abs((rec.predicted_for - ts).total_seconds())
                    if diff <= self._match_window.total_seconds():
                        rec.actual_value = actual_value
                        rec.error = actual_value - rec.predicted_value
                        break  # match first unresolved prediction

    # ------------------------------------------------------------------
    # Read side
    # ------------------------------------------------------------------

    async def get_accuracy(
        self,
        service_name: str,
        metric_name: str,
        n_recent: int = 100,
    ) -> dict:
        """Return MAE, RMSE, MAPE, and sample count for recent predictions."""
        key = (service_name, metric_name)
        async with self._lock:
            preds = self._preds.get(key)
            if not preds:
                return {"mae": None, "rmse": None, "mape": None, "n": 0}
            evaluated = [r for r in preds if r.error is not None][-n_recent:]

        if not evaluated:
            return {"mae": None, "rmse": None, "mape": None, "n": 0}

        errors = [r.error for r in evaluated]  # type: ignore[misc]
        mae = sum(abs(e) for e in errors) / len(errors)
        rmse = math.sqrt(sum(e**2 for e in errors) / len(errors))

        # MAPE: skip zero actuals to avoid division by zero
        mape_samples = [
            abs(r.error / r.actual_value)
            for r in evaluated
            if r.actual_value and abs(r.actual_value) > 1e-9
        ]
        mape = (sum(mape_samples) / len(mape_samples) * 100) if mape_samples else None

        return {
            "mae": round(mae, 4),
            "rmse": round(rmse, 4),
            "mape": round(mape, 2) if mape is not None else None,
            "n": len(evaluated),
        }

    async def list_services(self) -> List[Tuple[str, str]]:
        """Return all (service_name, metric_name) pairs with tracked data."""
        async with self._lock:
            return list(self._preds.keys())
