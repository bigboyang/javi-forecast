"""Exponential Weighted Moving Average (EWMA) forecaster.

Pure-numpy implementation – no statsmodels dependency.
Suitable for low-latency, real-time forecasting where the series is
roughly stationary or slowly drifting.

Algorithm
---------
* Level:  L_t = α·x_t + (1-α)·L_{t-1}
* Trend:  T_t = β·(L_t - L_{t-1}) + (1-β)·T_{t-1}  (double-EWMA / Holt)
* h-step forecast: F_{t+h} = L_t + h·T_t
* Confidence interval: ±z * σ_residual * sqrt(h)
"""

from typing import Tuple

import numpy as np

from .base import BaseForecaster


class EWMAForecaster(BaseForecaster):
    """Double-EWMA (Holt linear trend) forecaster.

    Parameters
    ----------
    alpha : float
        Smoothing factor for the level (0 < alpha < 1).
        Higher → more weight to recent observations.
    beta : float
        Smoothing factor for the trend (0 < beta < 1).
    confidence_z : float
        Z-multiplier for the confidence interval (default 1.96 → 95 %).
    """

    def __init__(
        self,
        alpha: float = 0.3,
        beta: float = 0.1,
        confidence_z: float = 1.96,
    ) -> None:
        if not (0 < alpha < 1):
            raise ValueError("alpha must be in (0, 1)")
        if not (0 < beta < 1):
            raise ValueError("beta must be in (0, 1)")
        self.alpha = alpha
        self.beta = beta
        self.confidence_z = confidence_z

        # State set by fit()
        self._level: float = 0.0
        self._trend: float = 0.0
        self._sigma: float = 1.0
        self._fitted: bool = False

    # ------------------------------------------------------------------
    # BaseForecaster interface
    # ------------------------------------------------------------------

    def fit(self, values: np.ndarray) -> "EWMAForecaster":
        values = np.asarray(values, dtype=float)
        if len(values) < 2:
            raise ValueError("EWMA requires at least 2 data points")

        level = values[0]
        trend = values[1] - values[0]
        residuals: list[float] = []

        for x in values[1:]:
            prev_level = level
            level = self.alpha * x + (1 - self.alpha) * (level + trend)
            trend = self.beta * (level - prev_level) + (1 - self.beta) * trend
            residuals.append(x - (prev_level + trend))

        self._level = level
        self._trend = trend
        self._sigma = float(np.std(residuals)) if residuals else 1.0
        if self._sigma == 0:
            self._sigma = 1e-6
        self._fitted = True
        return self

    def predict(
        self, steps: int
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        if not self._fitted:
            raise RuntimeError("Model has not been fitted yet")
        if steps < 1:
            raise ValueError("steps must be >= 1")

        h = np.arange(1, steps + 1, dtype=float)
        predicted = self._level + h * self._trend
        margin = self.confidence_z * self._sigma * np.sqrt(h)
        lower = predicted - margin
        upper = predicted + margin

        # Clamp lower bound to 0 for non-negative metrics (e.g. latency)
        lower = np.maximum(lower, 0.0)
        return predicted, lower, upper
