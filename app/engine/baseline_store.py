"""Hour-of-week baseline store.

Maintains per-(service, metric, weekday, hour) rolling statistics using
Welford's online algorithm.  The AnomalyPredictor can use these baselines
to normalise predictions against the expected distribution for the same
time-of-day / day-of-week slot, dramatically reducing false positives from
predictable traffic patterns (Monday-morning spikes, nightly quiet hours, etc.).
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime

import numpy as np

logger = logging.getLogger(__name__)

# Minimum per-bucket observations before the baseline is trusted
_MIN_BUCKET_OBS = 5

# (service, metric, weekday 0-6, hour 0-23)
_Key = tuple[str, str, int, int]


class _WelfordStats:
    """Incremental mean and variance via Welford's online algorithm."""

    __slots__ = ("n", "mean", "M2")

    def __init__(self) -> None:
        self.n: int = 0
        self.mean: float = 0.0
        self.M2: float = 0.0

    def update(self, value: float) -> None:
        self.n += 1
        delta = value - self.mean
        self.mean += delta / self.n
        delta2 = value - self.mean
        self.M2 += delta * delta2

    @property
    def variance(self) -> float:
        return self.M2 / self.n if self.n > 1 else 0.0

    @property
    def std(self) -> float:
        return float(np.sqrt(self.variance))


class BaselineStore:
    """Hour-of-week bucketed baseline statistics for anomaly normalisation.

    Each unique (service, metric, weekday, hour) slot maintains a running
    (mean, std) via Welford's online algorithm so that Z-scores are computed
    against the typical distribution for that time slot rather than the
    global historical window.

    Parameters
    ----------
    min_observations:
        Minimum samples required in a bucket before it is used for
        normalisation.  Slots with fewer observations fall back to the
        global mean/std computed over the full window.
    """

    def __init__(self, min_observations: int = _MIN_BUCKET_OBS) -> None:
        self.min_observations = min_observations
        self._buckets: dict[_Key, _WelfordStats] = defaultdict(_WelfordStats)

    def update(
        self,
        service: str,
        metric: str,
        timestamp: datetime,
        value: float,
    ) -> None:
        """Record a single observation into the appropriate hour-of-week bucket."""
        key: _Key = (service, metric, timestamp.weekday(), timestamp.hour)
        self._buckets[key].update(value)

    def get_baseline(
        self,
        service: str,
        metric: str,
        weekday: int,
        hour: int,
    ) -> tuple[float, float] | None:
        """Return ``(mean, std)`` for the given slot, or ``None`` if insufficient data.

        Parameters
        ----------
        weekday:
            0 = Monday … 6 = Sunday  (matches ``datetime.weekday()``).
        hour:
            0–23 UTC hour of the predicted timestamp.
        """
        key: _Key = (service, metric, weekday, hour)
        stats = self._buckets.get(key)
        if stats is None or stats.n < self.min_observations:
            return None
        return (stats.mean, max(stats.std, 1e-6))

    def populated_slots(self, service: str, metric: str) -> int:
        """Return count of (weekday, hour) slots with sufficient data."""
        return sum(
            1
            for (svc, met, _, _), stats in self._buckets.items()
            if svc == service and met == metric and stats.n >= self.min_observations
        )
