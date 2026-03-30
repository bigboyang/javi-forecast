"""Model selector: picks the best forecasting model by train/test MSE.

When ``model_name == "auto"`` all three models are fitted on the
historical series and the one with the lowest MSE on a held-out test
split is returned.  For explicit model names the corresponding class is
returned immediately without any evaluation overhead.
"""

import logging
from typing import Tuple

import numpy as np

from .models.base import BaseForecaster
from .models.ewma import EWMAForecaster
from .models.arima import ARIMAForecaster
from .models.holtwinters import HoltWintersForecaster

logger = logging.getLogger(__name__)

_MODEL_CLASSES = {
    "ewma": EWMAForecaster,
    "arima": ARIMAForecaster,
    "holtwinters": HoltWintersForecaster,
}


def select_model(
    values: np.ndarray,
    model_name: str = "auto",
) -> Tuple[BaseForecaster, float]:
    """Return *(fitted_model, mse)* for the chosen (or best) model.

    Parameters
    ----------
    values:
        1-D array of historical observations.
    model_name:
        ``"auto"`` or one of ``"ewma" | "arima" | "holtwinters"``.

    Returns
    -------
    model : BaseForecaster
        Already fitted on the full *values* array.
    mse : float
        Cross-validation MSE (may be ``inf`` if evaluation failed).
    """
    values = np.asarray(values, dtype=float)

    if model_name != "auto":
        cls = _MODEL_CLASSES.get(model_name)
        if cls is None:
            logger.warning("Unknown model '%s', falling back to ewma", model_name)
            cls = EWMAForecaster
        model = cls()
        mse = model.mse(values)
        model.fit(values)
        return model, mse

    # --- Auto mode: evaluate all candidates ---------------------------
    best_model: BaseForecaster = EWMAForecaster()
    best_mse = float("inf")
    best_name = "ewma"

    candidates = [
        ("ewma", EWMAForecaster()),
        ("arima", ARIMAForecaster()),
        ("holtwinters", HoltWintersForecaster()),
    ]

    for name, candidate in candidates:
        try:
            mse = candidate.mse(values)
            logger.debug("model=%s mse=%.4f", name, mse)
            if mse < best_mse:
                best_mse = mse
                best_model = candidate
                best_name = name
        except Exception as exc:
            logger.warning("model=%s evaluation error: %s", name, exc)

    logger.info(
        "auto model selection: best=%s mse=%.4f", best_name, best_mse
    )

    # Re-fit best model on full dataset
    best_model.fit(values)
    return best_model, best_mse
