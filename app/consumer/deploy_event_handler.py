"""Deployment event handler: routes DeploymentEvent to DeploymentStore.

Called by KafkaConsumerService when a message arrives on the deploys topic,
and by the HTTP ingest endpoint (POST /v1/events/deploy fallback).
"""

from __future__ import annotations

import logging
import time

from ..engine.deployment_store import DeploymentStore
from ..models.deployment import DeploymentEvent

logger = logging.getLogger(__name__)


class DeployEventHandler:
    """Routes DeploymentEvent to the DeploymentStore."""

    def __init__(self, store: DeploymentStore) -> None:
        self._store = store

    async def handle(self, event: DeploymentEvent) -> None:
        if event.timestamp_ms == 0:
            event.timestamp_ms = int(time.time() * 1000)
        try:
            self._store.add(event)
            logger.info(
                "deploy event ingested service=%s version=%s env=%s",
                event.service_name,
                event.version,
                event.environment,
            )
        except Exception as exc:
            logger.error(
                "failed to handle deploy event service=%s: %s",
                event.service_name,
                exc,
            )
