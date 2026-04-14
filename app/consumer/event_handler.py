"""Event handler: routes incoming span events to the feature store.

Called by both the Kafka consumer (async loop) and the HTTP ingest
endpoint so that all span ingestion goes through a single code path.
"""

from __future__ import annotations

import logging
from typing import Optional

from ..engine.feature_store import FeatureStore
from ..engine.service_registry import ServiceRegistry
from ..engine.span_topology import SpanTopologyTracker
from ..models.span import SpanEvent

logger = logging.getLogger(__name__)


class EventHandler:
    """Routes span events to the FeatureStore and topology tracker.

    Parameters
    ----------
    feature_store:
        Shared FeatureStore instance.
    topology_tracker:
        Optional SpanTopologyTracker for real-time service call graph.
    """

    def __init__(
        self,
        feature_store: FeatureStore,
        topology_tracker: Optional[SpanTopologyTracker] = None,
        service_registry: Optional[ServiceRegistry] = None,
    ) -> None:
        self._store = feature_store
        self._topology = topology_tracker
        self._registry = service_registry

    async def handle(self, span: SpanEvent) -> None:
        """Process a single span event.

        Parameters
        ----------
        span:
            The span event to ingest.
        """
        try:
            await self._store.update(span)
            if self._topology is not None:
                self._topology.record(span)
            if self._registry is not None:
                self._registry.update_from_span(span)
            logger.debug(
                "span ingested service=%s trace_id=%s duration_ms=%.2f is_error=%s",
                span.service_name,
                span.trace_id,
                span.duration_ms,
                span.is_error,
            )
        except Exception as exc:
            logger.error(
                "failed to handle span trace_id=%s service=%s: %s",
                span.trace_id,
                span.service_name,
                exc,
            )
