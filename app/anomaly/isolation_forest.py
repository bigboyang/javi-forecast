"""Isolation Forest multivariate anomaly detector.

Trains a scikit-learn IsolationForest on the five-dimensional RED metric
vector  [rate, error_rate, p50_ms, p95_ms, p99_ms]  for each service.

Catches anomalies that are "globally unusual" even when no individual
metric crosses its Z-score threshold — e.g. a simultaneous rise in error
rate AND p99 latency that individually look mild but together signal a
cascading failure.

Model lifecycle
---------------
* The detector is **stateful per service**: each service has its own fitted
  ``IsolationForest`` instance.
* Models are re-fit every ``refit_interval_minutes`` (default 30) or when
  the sample count has grown by more than ``refit_growth_factor`` (default 2×).
* Fitting runs in a thread-pool executor to keep the asyncio event loop free.
* At least ``min_samples`` RED metric vectors are required before fitting.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import numpy as np

from ..anomaly.predictor import AnomalyPrediction
from ..models.metric import REDMetric

logger = logging.getLogger(__name__)

# Minimum RED vectors per service before we attempt a fit
_MIN_SAMPLES = 50

# Isolation Forest contamination assumption (5 % of points are anomalies)
_CONTAMINATION = 0.05

# Score below which we classify a point as an anomaly (-1 = definitely anomaly)
# IsolationForest.score_samples() returns negative; more negative = more anomalous.
# Empirically, threshold = -0.6 catches clear outliers while avoiding false positives.
_SCORE_THRESHOLD_CRITICAL = -0.65
_SCORE_THRESHOLD_WARN = -0.55

# How often to refit (in minutes of wall time)
_REFIT_INTERVAL_MINUTES = 30


class _ServiceModel:
    """Holds a fitted IsolationForest + metadata for one service."""

    __slots__ = ("model", "scaler_mean", "scaler_std", "fitted_at", "n_samples")

    def __init__(self, model, scaler_mean, scaler_std, n_samples: int) -> None:
        self.model = model
        self.scaler_mean: np.ndarray = scaler_mean
        self.scaler_std: np.ndarray = scaler_std
        self.fitted_at: datetime = datetime.now(tz=UTC)
        self.n_samples: int = n_samples


def _build_matrix(metrics: list[REDMetric]) -> np.ndarray:
    """Convert a list of REDMetric objects to an (N, 5) float array."""
    return np.array(
        [
            [m.rate, m.error_rate, m.p50_ms, m.p95_ms, m.p99_ms]
            for m in metrics
        ],
        dtype=float,
    )


def _standardise(
    X: np.ndarray,
    mean: np.ndarray | None = None,
    std: np.ndarray | None = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Z-score standardise X column-wise.

    If *mean* and *std* are provided they are used directly (for transform-only
    calls); otherwise they are computed from X (for fit+transform calls).
    """
    if mean is None or std is None:
        mean = X.mean(axis=0)
        std = X.std(axis=0)
    std = np.where(std < 1e-6, 1e-6, std)
    return (X - mean) / std, mean, std


class IsolationForestDetector:
    """Per-service Isolation Forest multivariate anomaly detector.

    Parameters
    ----------
    contamination:
        Expected fraction of anomalies in the training data.
    n_estimators:
        Number of trees in the IsolationForest ensemble.
    min_samples:
        Minimum RED metric vectors required before fitting.
    refit_interval_minutes:
        Minimum wall-clock time between consecutive model refits.
    """

    def __init__(
        self,
        contamination: float = _CONTAMINATION,
        n_estimators: int = 100,
        min_samples: int = _MIN_SAMPLES,
        refit_interval_minutes: float = _REFIT_INTERVAL_MINUTES,
    ) -> None:
        self.contamination = contamination
        self.n_estimators = n_estimators
        self.min_samples = min_samples
        self.refit_interval_minutes = refit_interval_minutes

        self._models: dict[str, _ServiceModel] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fit_and_predict(
        self,
        service: str,
        metrics: list[REDMetric],
    ) -> AnomalyPrediction | None:
        """Fit (or reuse) a model for *service* and score the latest vector.

        Returns an ``AnomalyPrediction`` if the most-recent RED snapshot is
        anomalous, otherwise ``None``.

        This method is **synchronous** and CPU-bound; callers should run it
        inside ``asyncio.get_event_loop().run_in_executor(None, ...)``.
        """
        if len(metrics) < self.min_samples:
            return None

        try:
            from sklearn.ensemble import IsolationForest  # lazy import
        except ImportError:
            logger.warning(
                "scikit-learn not installed – IsolationForestDetector disabled"
            )
            return None

        X = _build_matrix(metrics)
        X_std, mean, std = _standardise(X)

        existing = self._models.get(service)
        need_refit = existing is None or self._should_refit(existing, len(metrics))

        if need_refit:
            logger.debug(
                "IsolationForest: fitting service=%s n_samples=%d", service, len(X)
            )
            clf = IsolationForest(
                n_estimators=self.n_estimators,
                contamination=self.contamination,
                random_state=42,
                n_jobs=1,
            )
            clf.fit(X_std)
            self._models[service] = _ServiceModel(
                model=clf,
                scaler_mean=mean,
                scaler_std=std,
                n_samples=len(X),
            )
            existing = self._models[service]

        # Score the latest point using the fitted model's scaler
        latest = X[-1:]
        latest_std = (latest - existing.scaler_mean) / np.where(
            existing.scaler_std < 1e-6, 1e-6, existing.scaler_std
        )
        score = float(existing.model.score_samples(latest_std)[0])

        severity: str | None = None
        if score <= _SCORE_THRESHOLD_CRITICAL:
            severity = "critical"
        elif score <= _SCORE_THRESHOLD_WARN:
            severity = "warn"

        if severity is None:
            return None

        # Identify the most deviant dimension for logging / context
        latest_vec = X[-1]
        global_mean = X[:-1].mean(axis=0)
        global_std = np.where(X[:-1].std(axis=0) < 1e-6, 1e-6, X[:-1].std(axis=0))
        dim_z = np.abs((latest_vec - global_mean) / global_std)
        dim_names = ["rate", "error_rate", "p50_ms", "p95_ms", "p99_ms"]
        top_dim = dim_names[int(np.argmax(dim_z))]
        top_z = float(dim_z.max())

        now = datetime.now(tz=UTC)
        logger.warning(
            "IsolationForest anomaly service=%s score=%.3f top_dim=%s z=%.2f severity=%s",
            service,
            score,
            top_dim,
            top_z,
            severity,
        )
        return AnomalyPrediction(
            service_name=service,
            metric_name=f"multivariate_red:{top_dim}",
            severity=severity,
            predicted_value=float(latest_vec[dim_names.index(top_dim)]),
            expected_value=float(global_mean[dim_names.index(top_dim)]),
            z_score=top_z,
            predicted_at=now,
            seconds_until_anomaly=0.0,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _should_refit(self, model: _ServiceModel, current_n: int) -> bool:
        now = datetime.now(tz=UTC)
        elapsed_minutes = (now - model.fitted_at).total_seconds() / 60.0
        if elapsed_minutes >= self.refit_interval_minutes:
            return True
        # Also refit if sample count has grown significantly (≥ 2×)
        if current_n >= model.n_samples * 2:
            return True
        return False
