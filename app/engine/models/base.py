from abc import ABC, abstractmethod

import numpy as np


class BaseForecaster(ABC):
    """Abstract base class for all forecasting models.

    Subclasses must implement fit() and predict().
    The interface mirrors a lightweight sklearn-style estimator adapted for
    time-series forecasting with confidence intervals.
    """

    @abstractmethod
    def fit(self, values: np.ndarray) -> "BaseForecaster":
        """Fit the model on an array of historical observations.

        Parameters
        ----------
        values:
            1-D array of floats in chronological order.

        Returns
        -------
        self – for method chaining.
        """

    @abstractmethod
    def predict(
        self, steps: int
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Generate *steps* future predictions.

        Returns
        -------
        predicted : np.ndarray, shape (steps,)
            Point forecast.
        lower : np.ndarray, shape (steps,)
            Lower confidence bound.
        upper : np.ndarray, shape (steps,)
            Upper confidence bound.
        """

    def mse(self, values: np.ndarray, test_fraction: float = 0.2) -> float:
        """Compute MSE via a simple train/test split.

        Uses the last ``ceil(len(values) * test_fraction)`` points as the
        test set.  If there are fewer than 4 points the method returns inf.
        """
        n = len(values)
        n_test = max(1, int(np.ceil(n * test_fraction)))
        n_train = n - n_test
        if n_train < 3:
            return float("inf")

        train = values[:n_train]
        test = values[n_train:]
        try:
            self.fit(train)
            predicted, _, _ = self.predict(n_test)
            return float(np.mean((predicted - test) ** 2))
        except Exception:
            return float("inf")
