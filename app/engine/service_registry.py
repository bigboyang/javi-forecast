"""Service metadata registry.

Tracks OTel Resource attributes per service extracted from incoming spans:
  - service.version
  - deployment.environment
  - k8s.pod.name / k8s.namespace.name
  - cloud.region
  - host.name

This fills the gap identified in the APM comparison analysis:
Datadog/Dynatrace use these attributes for version-based deployment tracking,
environment isolation, and infrastructure correlation.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from ..models.span import SpanEvent

# Well-known OTel Resource semantic convention keys we track explicitly
_KNOWN_RESOURCE_KEYS = frozenset(
    (
        "service.version",
        "deployment.environment",
        "k8s.pod.name",
        "k8s.namespace.name",
        "cloud.region",
        "host.name",
    )
)


@dataclass
class ServiceMetadata:
    service_name: str
    service_version: str | None = None
    deployment_environment: str | None = None
    k8s_pod_name: str | None = None
    k8s_namespace: str | None = None
    cloud_region: str | None = None
    host_name: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)
    last_seen: datetime = field(
        default_factory=lambda: datetime.now(tz=UTC)
    )


class ServiceRegistry:
    """Thread-safe registry of OTel Resource attributes per service.

    Updated by :class:`~app.consumer.event_handler.EventHandler` on every
    span ingestion.  The registry retains the *most-recently-seen* value for
    each attribute, so a rolling-update deployment will transition the
    version tag as new pods come up.
    """

    def __init__(self) -> None:
        self._registry: dict[str, ServiceMetadata] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def update_from_span(self, span: SpanEvent) -> None:
        """Extract resource attributes from *span* and update the registry.

        No-ops if ``span.resource_attributes`` is empty.
        """
        attrs = span.resource_attributes
        if not attrs:
            return

        with self._lock:
            meta = self._registry.get(span.service_name)
            if meta is None:
                meta = ServiceMetadata(service_name=span.service_name)
                self._registry[span.service_name] = meta

            if "service.version" in attrs:
                meta.service_version = str(attrs["service.version"])
            if "deployment.environment" in attrs:
                meta.deployment_environment = str(attrs["deployment.environment"])
            if "k8s.pod.name" in attrs:
                meta.k8s_pod_name = str(attrs["k8s.pod.name"])
            if "k8s.namespace.name" in attrs:
                meta.k8s_namespace = str(attrs["k8s.namespace.name"])
            if "cloud.region" in attrs:
                meta.cloud_region = str(attrs["cloud.region"])
            if "host.name" in attrs:
                meta.host_name = str(attrs["host.name"])

            # Preserve any extra resource keys not explicitly mapped above
            for k, v in attrs.items():
                if k not in _KNOWN_RESOURCE_KEYS:
                    meta.extra[k] = v

            meta.last_seen = datetime.now(tz=UTC)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get(self, service_name: str) -> ServiceMetadata | None:
        """Return metadata for a single service, or None if unknown."""
        with self._lock:
            return self._registry.get(service_name)

    def get_all(self) -> list[ServiceMetadata]:
        """Return metadata for all registered services."""
        with self._lock:
            return list(self._registry.values())

    def service_count(self) -> int:
        with self._lock:
            return len(self._registry)
