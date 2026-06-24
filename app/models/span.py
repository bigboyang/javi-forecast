from typing import Any

from pydantic import BaseModel


class SpanEvent(BaseModel):
    schema_version: str | None = None
    trace_id: str
    span_id: str
    parent_span_id: str | None = None
    name: str
    kind: int = 0
    start_time_nano: int
    end_time_nano: int
    attributes: dict[str, Any] = {}
    resource_attributes: dict[str, Any] = {}  # OTel Resource attributes (service.version, k8s.*, etc.)
    status_code: int = 0   # 0=UNSET, 1=OK, 2=ERROR
    status_message: str = ""
    service_name: str
    scope_name: str = ""

    @property
    def duration_ms(self) -> float:
        return (self.end_time_nano - self.start_time_nano) / 1_000_000

    @property
    def is_error(self) -> bool:
        return self.status_code == 2


class SpanBatch(BaseModel):
    spans: list[SpanEvent]
