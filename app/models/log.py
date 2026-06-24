from pydantic import BaseModel
from typing import Any, Dict, List, Optional


class LogEvent(BaseModel):
    """Single OTel log record published by javi-collector's LogProducer.

    JSON field names match the Go ``LogEvent`` struct so the Kafka
    message can be deserialised without transformation.

    severity values: TRACE | DEBUG | INFO | WARN | ERROR | FATAL
    """

    schema_version: Optional[str] = None
    service_name: str
    severity: str                               # log level
    body: str                                   # log message text
    timestamp_ms: int                           # epoch milliseconds
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    attributes: Optional[Dict[str, Any]] = {}


class LogEventBatch(BaseModel):
    """Batch wrapper used by the HTTP ingest endpoint."""

    logs: List[LogEvent]
