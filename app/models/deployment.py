"""Deployment event model.

Published by CI/CD pipelines (GitHub Actions, ArgoCD) via
javi-collector POST /v1/events/deploy → Kafka "deploys" → javi-forecast.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class DeploymentEvent(BaseModel):
    schema_version: Optional[str] = None
    service_name: str
    version: str
    environment: str = ""
    deployed_by: Optional[str] = None
    timestamp_ms: int = Field(default=0)
    metadata: Dict[str, Any] = Field(default_factory=dict)
