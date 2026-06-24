"""Deployment event model.

Published by CI/CD pipelines (GitHub Actions, ArgoCD) via
javi-collector POST /v1/events/deploy → Kafka "deploys" → javi-forecast.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class DeploymentEvent(BaseModel):
    schema_version: str | None = None
    service_name: str
    version: str
    environment: str = ""
    deployed_by: str | None = None
    timestamp_ms: int = Field(default=0)
    metadata: dict[str, Any] = Field(default_factory=dict)
