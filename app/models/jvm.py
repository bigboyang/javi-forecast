"""Pydantic models for JVM metric ingestion."""

from __future__ import annotations

from pydantic import BaseModel, Field


class JvmMetricEvent(BaseModel):
    """A single JVM metric snapshot from a monitored service.

    Collected every 60 seconds by ``JvmSnapshotSender`` in javi-agent
    and POST-ed directly to ``/v1/metrics/jvm``.
    """

    service_name: str
    timestamp_nano: int = Field(..., description="Epoch nanoseconds")

    # ---- Memory ----------------------------------------------------------------
    heap_used_bytes: float = Field(0.0, ge=0)
    heap_committed_bytes: float = Field(0.0, ge=0)
    heap_max_bytes: float = Field(0.0, ge=0)

    # ---- GC (delta since last report) ------------------------------------------
    gc_count_delta: int = Field(0, ge=0)
    gc_pause_ms_total_delta: float = Field(0.0, ge=0)
    gc_collection_name: str = ""

    # ---- Threads ----------------------------------------------------------------
    thread_count: int = Field(0, ge=0)
    thread_peak: int = Field(0, ge=0)
    thread_daemon: int = Field(0, ge=0)

    # ---- CPU (0.0 – 1.0) -------------------------------------------------------
    process_cpu_utilization: float = Field(0.0, ge=0.0, le=1.0)
    system_cpu_utilization: float = Field(0.0, ge=0.0, le=1.0)


class JvmMetricBatch(BaseModel):
    """Batch wrapper for JVM metric events."""

    metrics: list[JvmMetricEvent]
