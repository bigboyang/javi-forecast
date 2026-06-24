"""ARIMA forecaster backed by statsmodels.

Auto-selects (p, d, q) order by minimising AIC over the search space
p ∈ {0,1,2,3}, d ∈ {0,1}, q ∈ {0,1,2,3}.
The best model found during fit() is cached and reused for predict().
"""


import numpy as np

from .base import BaseForecaster


class ARIMAForecaster(BaseForecaster):
    """ARIMA forecaster with automatic order selection.

    Parameters
    ----------
    max_p, max_d, max_q : int
        Upper bounds for the (p, d, q) search grid.
    confidence_alpha : float
        Alpha level for confidence intervals (default 0.05 → 95 % CI).
    """

    def __init__(
        self,
        max_p: int = 3,
        max_d: int = 1,
        max_q: int = 3,
        confidence_alpha: float = 0.05,
    ) -> None:
        self.max_p = max_p
        self.max_d = max_d
        self.max_q = max_q
        self.confidence_alpha = confidence_alpha

        self._result = None          # fitted ARIMAResults
        self._best_order: tuple[int, int, int] | None = None
        self._fitted: bool = False

    # ------------------------------------------------------------------
    # BaseForecaster interface
    # ------------------------------------------------------------------

    def fit(self, values: np.ndarray) -> "ARIMAForecaster":
        from statsmodels.tsa.arima.model import ARIMA

        values = np.asarray(values, dtype=float)
        n = len(values)
        if n < 4:
            raise ValueError("ARIMA requires at least 4 data points")

        best_aic = float("inf")
        best_result = None
        best_order: tuple[int, int, int] = (1, 0, 0)

        for p in range(self.max_p + 1):
            for d in range(self.max_d + 1):
                for q in range(self.max_q + 1):
                    if p == 0 and q == 0:
                        continue  # degenerate model
                    try:
                        model = ARIMA(values, order=(p, d, q))
                        result = model.fit()
                        if result.aic < best_aic:
                            best_aic = result.aic
                            best_result = result
                            best_order = (p, d, q)
                    except Exception:
                        continue

        if best_result is None:
            # Fallback: AR(1)
            model = ARIMA(values, order=(1, 0, 0))
            best_result = model.fit()
            best_order = (1, 0, 0)

        self._result = best_result
        self._best_order = best_order
        self._fitted = True
        return self

    def predict(
        self, steps: int
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        if not self._fitted or self._result is None:
            raise RuntimeError("Model has not been fitted yet")
        if steps < 1:
            raise ValueError("steps must be >= 1")

        forecast = self._result.get_forecast(steps=steps)
        predicted = forecast.predicted_mean.values
        ci = forecast.conf_int(alpha=self.confidence_alpha).values
        lower = np.maximum(ci[:, 0], 0.0)
        upper = ci[:, 1]
        return predicted, lower, upper

    @property
    def best_order(self) -> tuple[int, int, int] | None:
        return self._best_order
