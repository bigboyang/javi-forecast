from pydantic import BaseModel
from typing import List
from datetime import datetime


class MetricPoint(BaseModel):
    timestamp: datetime
    value: float


class REDMetric(BaseModel):
    service_name: str
    timestamp: datetime
    rate: float           # requests per second
    error_rate: float     # 0.0 - 1.0
    p50_ms: float
    p95_ms: float
    p99_ms: float


class TimeSeriesData(BaseModel):
    service_name: str
    metric_name: str      # rate | error_rate | p50_ms | p95_ms | p99_ms
    points: List[MetricPoint]
