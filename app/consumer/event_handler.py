"""Event handler: routes incoming span events to the feature store.

Called by both the Kafka consumer (async loop) and the HTTP ingest
endpoint so that all span ingestion goes through a single code path.
"""

from __future__ import annotations

import logging

from ..engine.feature_store import FeatureStore
from ..models.span import SpanEvent

logger = logging.getLogger(__name__)

# Prometheus counter – imported lazily to avoid import-time side-effects
_spans_counter = None


def _get_counter():
    global _spans_counter
    if _spans_counter is None:
        try:
            from prometheus_client import Counter
            _spans_counter = Counter(
                "javi_forecast_spans_ingested_total",
                "Total span events ingested",
                ["service"],
            )
        except Exception:
            pass
    return _spans_counter


class EventHandler:
    """Routes span events to the FeatureStore.

    Parameters
    ----------
    feature_store:
        Shared FeatureStore instance.
    """

    def __init__(self, feature_store: FeatureStore) -> None:
        self._store = feature_store

    async def handle(self, span: SpanEvent) -> None:
        """Process a single span event.

        Parameters
        ----------
        span:
            The span event to ingest.
        """
        try:
            await self._store.update(span)
            counter = _get_counter()
            if counter is not None:
                counter.labels(service=span.service_name).inc()
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
