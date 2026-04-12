"""STL Decomposition-based anomaly detector.

Decomposes a time series into Trend + Seasonal + Residual components
using LOESS smoothing (statsmodels STL).  Flags the most-recent point
as anomalous when its residual z-score exceeds ``threshold_z``.

Characteristics
---------------
* **Current-point detection** (complementary to AnomalyPredictor which is
  forecast-based, and IsolationForest which is multivariate).
* **Seasonal-aware**: removes the expected hourly/daily pattern before
  scoring, reducing false positives from predictable traffic cycles.
* Requires at least ``2 × period`` data points to run.
* Uses statsmodels ``robust=True`` mode, which down-weights outliers
  during the LOESS fit – making the decomposition itself resistant to
  the very anomalies it is trying to detect.

Typical usage in the Forecaster
--------------------------------
::

    stl = STLAnomalyDetector(period=60)   # 60 min seasonality at 1-min cadence
    result = stl.detect(np.array(values))
    if result and result.is_anomaly:
        ...
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)

# Minimum data points = 2 complete seasonal periods
_MIN_PERIODS = 2
# Default seasonality: 60 one-minute buckets = 1 hour
_DEFAULT_PERIOD = 60
# Residual z-score above which we flag a point as anomalous
_DEFAULT_THRESHOLD_Z = 3.0


@dataclass
class STLAnomalyResult:
    is_anomaly: bool
    residual_z: float        # z-score of the latest residual
    trend: float             # trend component at the latest point
    seasonal: float          # seasonal component at the latest point
    residual: float          # raw residual at the latest point


class STLAnomalyDetector:
    """Seasonal-Trend decomposition anomaly detector.

    Parameters
    ----------
    period:
        Seasonality period in data points.
        Default 60 assumes 1-minute cadence with 1-hour seasonality.
    threshold_z:
        Residual z-score above which a point is flagged as anomalous.
    """

    def __init__(
        self,
        period: int = _DEFAULT_PERIOD,
        threshold_z: float = _DEFAULT_THRESHOLD_Z,
    ) -> None:
        self.period = period
        self.threshold_z = threshold_z

    def detect(self, values: np.ndarray) -> Optional[STLAnomalyResult]:
        """Run STL decomposition on *values* and score the final data point.

        Parameters
        ----------
        values:
            1-D float array of uniformly-spaced observations (oldest first).

        Returns
        -------
        STLAnomalyResult or None
            None is returned when there are insufficient data points or when
            the decomposition fails.
        """
        min_len = _MIN_PERIODS * self.period
        if len(values) < min_len:
            return None

        try:
            from statsmodels.tsa.seasonal import STL  # lazy import
            stl = STL(values, period=self.period, robust=True)
            fit = stl.fit()
        except Exception as exc:
            logger.debug("STL decomposition failed: %s", exc)
            return None

        residuals = fit.resid
        res_mean = float(np.mean(residuals))
        res_std = float(np.std(residuals))
        if res_std < 1e-9:
            res_std = 1e-9

        last_residual = float(residuals[-1])
        z = abs(last_residual - res_mean) / res_std

        return STLAnomalyResult(
            is_anomaly=z >= self.threshold_z,
            residual_z=round(z, 4),
            trend=round(float(fit.trend[-1]), 6),
            seasonal=round(float(fit.seasonal[-1]), 6),
            residual=round(last_residual, 6),
        )
