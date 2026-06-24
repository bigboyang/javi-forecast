from typing import Any

from pydantic import BaseModel


class LogEvent(BaseModel):
    """Single OTel log record published by javi-collector's LogProducer.

    JSON field names match the Go ``LogEvent`` struct so the Kafka
    message can be deserialised without transformation.

    severity values: TRACE | DEBUG | INFO | WARN | ERROR | FATAL
    """

    schema_version: str | None = None
    service_name: str
    severity: str                               # log level
    body: str                                   # log message text
    timestamp_ms: int                           # epoch milliseconds
    trace_id: str | None = None
    span_id: str | None = None
    attributes: dict[str, Any] | None = {}


class LogEventBatch(BaseModel):
    """Batch wrapper used by the HTTP ingest endpoint."""

    logs: list[LogEvent]
