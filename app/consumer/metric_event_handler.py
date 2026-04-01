"""Metric event handler: routes MetricEvent to MetricFeatureStore.

Called by KafkaConsumerService when a message arrives on the metrics
topic, and by the HTTP ingest endpoint (POST /v1/metrics).
"""

from __future__ import annotations

import logging

from ..engine.metric_feature_store import MetricFeatureStore
from ..models.metric import MetricEvent

logger = logging.getLogger(__name__)

_metrics_counter = None


def _get_counter():
    global _metrics_counter
    if _metrics_counter is None:
        try:
            from prometheus_client import Counter
            _metrics_counter = Counter(
                "javi_forecast_metrics_ingested_total",
                "Total OTel metric data-points ingested",
                ["service", "metric_name"],
            )
        except Exception:
            pass
    return _metrics_counter


class MetricEventHandler:
    """Routes MetricEvent to the MetricFeatureStore.

    Parameters
    ----------
    metric_store:
        Shared MetricFeatureStore instance.
    """

    def __init__(self, metric_store: MetricFeatureStore) -> None:
        self._store = metric_store

    async def handle(self, event: MetricEvent) -> None:
        """Process a single MetricEvent.

        Parameters
        ----------
        event:
            The metric event to ingest.
        """
        try:
            await self._store.update(event)
            counter = _get_counter()
            if counter is not None:
                counter.labels(
                    service=event.service_name,
                    metric_name=event.metric_name,
                ).inc()
        except Exception as exc:
            logger.error(
                "failed to handle metric service=%s metric=%s: %s",
                event.service_name,
                event.metric_name,
                exc,
            )
