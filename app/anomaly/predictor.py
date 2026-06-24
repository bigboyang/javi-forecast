"""Anomaly predictor: analyses forecast results for future anomaly risk.

Logic
-----
* Compute the mean and standard deviation of the historical values that
  were used to produce the forecast.
* For each future prediction point, compute a z-score relative to that
  historical distribution.
* Classify severity:
    - z >= 3 σ  → critical
    - z >= 2 σ  → warn
* Report the soonest predicted anomaly together with time-to-occurrence.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import numpy as np

from ..models.forecast import ForecastResult

if TYPE_CHECKING:
    from ..engine.baseline_store import BaselineStore

logger = logging.getLogger(__name__)

_WARN_Z = 2.0
_CRITICAL_Z = 3.0


@dataclass
class AnomalyPrediction:
    service_name: str
    metric_name: str
    severity: str                    # "warn" | "critical"
    predicted_value: float
    expected_value: float
    z_score: float
    predicted_at: datetime           # when the anomaly is predicted to occur
    detected_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))
    seconds_until_anomaly: float = 0.0


class AnomalyPredictor:
    """Stateless predictor that analyses a ForecastResult.

    Parameters
    ----------
    warn_z:
        Z-score threshold for a *warn* severity prediction.
    critical_z:
        Z-score threshold for a *critical* severity prediction.
    """

    def __init__(
        self,
        warn_z: float = _WARN_Z,
        critical_z: float = _CRITICAL_Z,
    ) -> None:
        self.warn_z = warn_z
        self.critical_z = critical_z

    def analyse(
        self,
        result: ForecastResult,
        historical_values: np.ndarray,
        baseline_store: BaselineStore | None = None,
    ) -> AnomalyPrediction | None:
        """Return the first (soonest) anomaly prediction, or None.

        Parameters
        ----------
        result:
            ForecastResult produced by the forecaster.
        historical_values:
            1-D array of the historical observations used to build the
            forecast.  Used to compute the global baseline distribution.
        baseline_store:
            Optional hour-of-week baseline.  When provided, each prediction
            point is normalised against its matching (weekday, hour) bucket
            rather than the global mean/std, reducing false positives from
            predictable traffic patterns.
        """
        if len(historical_values) < 2 or not result.predictions:
            return None

        global_mean = float(np.mean(historical_values))
        global_std = float(np.std(historical_values))
        if global_std == 0:
            global_std = 1e-6

        now = datetime.now(tz=UTC)

        for pt in result.predictions:
            # Use hour-of-week baseline if available for this slot
            mean, std = global_mean, global_std
            if baseline_store is not None and pt.timestamp.tzinfo is not None:
                slot = baseline_store.get_baseline(
                    result.service_name,
                    result.metric_name,
                    pt.timestamp.weekday(),
                    pt.timestamp.hour,
                )
                if slot is not None:
                    mean, std = slot

            z = (pt.predicted - mean) / std
            abs_z = abs(z)

            if abs_z >= self.critical_z:
                severity = "critical"
            elif abs_z >= self.warn_z:
                severity = "warn"
            else:
                continue

            # Make predicted_at timezone-aware if needed
            predicted_at = pt.timestamp
            if predicted_at.tzinfo is None:
                predicted_at = predicted_at.replace(tzinfo=UTC)

            secs = max(0.0, (predicted_at - now).total_seconds())
            logger.info(
                "anomaly predicted service=%s metric=%s severity=%s z=%.2f at=%s",
                result.service_name,
                result.metric_name,
                severity,
                abs_z,
                predicted_at.isoformat(),
            )
            return AnomalyPrediction(
                service_name=result.service_name,
                metric_name=result.metric_name,
                severity=severity,
                predicted_value=pt.predicted,
                expected_value=mean,
                z_score=float(abs_z),
                predicted_at=predicted_at,
                seconds_until_anomaly=secs,
            )

        return None

    def analyse_all(
        self,
        results: list[ForecastResult],
        historical_map: dict[str, np.ndarray],
        baseline_store: BaselineStore | None = None,
    ) -> list[AnomalyPrediction]:
        """Analyse a list of forecast results; return all anomaly predictions."""
        predictions: list[AnomalyPrediction] = []
        for result in results:
            key = f"{result.service_name}:{result.metric_name}"
            historical = historical_map.get(key)
            if historical is None:
                continue
            prediction = self.analyse(result, historical, baseline_store=baseline_store)
            if prediction:
                predictions.append(prediction)
        return predictions
