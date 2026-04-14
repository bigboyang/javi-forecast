"""Metric event handler: routes MetricEvent to MetricFeatureStore.

Called by KafkaConsumerService when a message arrives on the metrics
topic, and by the HTTP ingest endpoint (POST /v1/metrics).
"""

from __future__ import annotations

import logging

from ..engine.metric_feature_store import MetricFeatureStore
from ..models.metric import MetricEvent

logger = logging.getLogger(__name__)


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
        except Exception as exc:
            logger.error(
                "failed to handle metric service=%s metric=%s: %s",
                event.service_name,
                event.metric_name,
                exc,
            )
