"""Holt-Winters Triple Exponential Smoothing forecaster.

Uses ``statsmodels.tsa.holtwinters.ExponentialSmoothing`` with additive
trend and additive seasonal components.  Seasonal period defaults to 60
(one hour if data is sampled at 1-minute resolution) and falls back to
additive-trend only (Holt's linear method) when the series is shorter than
two full seasonal cycles.
"""

from typing import Optional, Tuple

import numpy as np

from .base import BaseForecaster

_DEFAULT_SEASONAL_PERIOD = 60   # 1 h at 1-min cadence


class HoltWintersForecaster(BaseForecaster):
    """Holt-Winters forecaster.

    Parameters
    ----------
    seasonal_periods : int
        Number of observations per seasonal cycle.
    confidence_alpha : float
        Alpha for confidence intervals (0.05 → 95 % CI).
    """

    def __init__(
        self,
        seasonal_periods: int = _DEFAULT_SEASONAL_PERIOD,
        confidence_alpha: float = 0.05,
    ) -> None:
        self.seasonal_periods = seasonal_periods
        self.confidence_alpha = confidence_alpha

        self._result = None
        self._sigma: float = 1.0
        self._fitted: bool = False
        self._used_seasonal: bool = False

    # ------------------------------------------------------------------
    # BaseForecaster interface
    # ------------------------------------------------------------------

    def fit(self, values: np.ndarray) -> "HoltWintersForecaster":
        from statsmodels.tsa.holtwinters import ExponentialSmoothing

        values = np.asarray(values, dtype=float)
        n = len(values)
        if n < 4:
            raise ValueError("HoltWinters requires at least 4 data points")

        # Use seasonal model only when we have at least 2 full cycles.
        use_seasonal = n >= 2 * self.seasonal_periods

        try:
            if use_seasonal:
                model = ExponentialSmoothing(
                    values,
                    trend="add",
                    seasonal="add",
                    seasonal_periods=self.seasonal_periods,
                    initialization_method="estimated",
                )
            else:
                model = ExponentialSmoothing(
                    values,
                    trend="add",
                    seasonal=None,
                    initialization_method="estimated",
                )
            self._result = model.fit(optimized=True, remove_bias=True)
            self._used_seasonal = use_seasonal
        except Exception:
            # Last-resort: simple Holt
            model = ExponentialSmoothing(
                values,
                trend="add",
                seasonal=None,
                initialization_method="heuristic",
            )
            self._result = model.fit()
            self._used_seasonal = False

        fitted_vals = self._result.fittedvalues
        residuals = values[len(values) - len(fitted_vals):] - fitted_vals
        self._sigma = float(np.std(residuals)) if len(residuals) > 0 else 1.0
        if self._sigma == 0:
            self._sigma = 1e-6
        self._fitted = True
        return self

    def predict(
        self, steps: int
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        if not self._fitted or self._result is None:
            raise RuntimeError("Model has not been fitted yet")
        if steps < 1:
            raise ValueError("steps must be >= 1")

        from scipy.stats import norm

        predicted = self._result.forecast(steps)
        if hasattr(predicted, "values"):
            predicted = predicted.values

        # Statsmodels HW does not produce native CI; approximate via
        # Gaussian propagation of in-sample residual std.
        z = float(norm.ppf(1 - self.confidence_alpha / 2))
        h = np.arange(1, steps + 1, dtype=float)
        margin = z * self._sigma * np.sqrt(h)
        lower = np.maximum(predicted - margin, 0.0)
        upper = predicted + margin
        return predicted, lower, upper

    @property
    def used_seasonal(self) -> bool:
        return self._used_seasonal
