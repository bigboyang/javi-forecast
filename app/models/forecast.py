from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel


class ModelType(StrEnum):
    EWMA = "ewma"
    ARIMA = "arima"
    HOLTWINTERS = "holtwinters"
    AUTO = "auto"
    VAR = "var"


class PredictionPoint(BaseModel):
    timestamp: datetime
    predicted: float
    lower_bound: float
    upper_bound: float
    confidence: float    # 0.0 - 1.0


class ForecastResult(BaseModel):
    service_name: str
    metric_name: str
    model_used: ModelType
    generated_at: datetime
    horizon_minutes: int
    predictions: list[PredictionPoint]
    mse: float | None = None
    is_anomaly_predicted: bool = False
    anomaly_severity: str | None = None   # warn | critical


class ForecastRequest(BaseModel):
    service_name: str
    metric_name: str = "p95_ms"
    horizon_minutes: int = 30
    model: ModelType = ModelType.AUTO


class CapacityForecast(BaseModel):
    service_name: str
    current_rate: float
    predicted_peak_rate: float
    predicted_peak_time: datetime
    saturation_risk: str   # low | medium | high | critical
    recommendation: str
