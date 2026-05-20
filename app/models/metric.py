from pydantic import BaseModel
from typing import Any, Dict, List, Optional
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


class MetricEvent(BaseModel):
    """Single OTel metric data-point published by javi-collector's MetricProducer.

    JSON field names match the Go ``MetricEvent`` struct so the Kafka
    message can be deserialised without transformation.

    metric_type values: GAUGE | SUM | HISTOGRAM
    """

    schema_version: Optional[str] = None
    service_name: str
    metric_name: str
    metric_type: str
    unit: Optional[str] = ""
    value: float
    timestamp_ms: int                       # epoch milliseconds
    attributes: Optional[Dict[str, Any]] = {}


class MetricEventBatch(BaseModel):
    """Batch wrapper used by the HTTP ingest endpoint."""

    metrics: List[MetricEvent]
