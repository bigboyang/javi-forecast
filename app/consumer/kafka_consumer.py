"""Async Kafka consumer service.

Consumes JSON-encoded span messages from the ``spans.error`` topic and
forwards each span to the :class:`EventHandler` for feature-store
ingestion.

Message format
--------------
Each message value is a JSON object that can be deserialised into a
:class:`~app.models.span.SpanEvent` (single span) OR a
:class:`~app.models.span.SpanBatch` (list under ``"spans"`` key).
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional

from ..config import settings
from ..models.span import SpanBatch, SpanEvent
from .event_handler import EventHandler

logger = logging.getLogger(__name__)

_POLL_TIMEOUT_MS = 1000
_MAX_BATCH_SIZE = 500


class KafkaConsumerService:
    """Manages the aiokafka consumer lifecycle.

    Parameters
    ----------
    event_handler:
        Handler that processes individual span events.
    brokers:
        Comma-separated Kafka broker addresses.
    topics:
        Comma-separated list of Kafka topics to subscribe to.
        Defaults to ``KAFKA_TOPICS`` setting (e.g. ``"spans.all"``).
        Use ``spans.all`` for accurate RED metrics (rate + latency + errors).
    group_id:
        Consumer group ID.
    """

    def __init__(
        self,
        event_handler: EventHandler,
        brokers: Optional[str] = None,
        topics: Optional[str] = None,
        group_id: Optional[str] = None,
    ) -> None:
        self._handler = event_handler
        self._brokers = brokers or settings.KAFKA_BROKERS
        self._topics = [t.strip() for t in (topics or settings.KAFKA_TOPICS).split(",") if t.strip()]
        self._group_id = group_id or settings.KAFKA_GROUP_ID
        self._consumer = None
        self._task: Optional[asyncio.Task] = None
        self._running = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the consumer and background polling task."""
        from aiokafka import AIOKafkaConsumer

        self._consumer = AIOKafkaConsumer(
            *self._topics,
            bootstrap_servers=self._brokers,
            group_id=self._group_id,
            value_deserializer=lambda v: v,   # raw bytes, parse below
            auto_offset_reset="latest",
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            max_poll_records=_MAX_BATCH_SIZE,
            session_timeout_ms=30000,
            heartbeat_interval_ms=3000,
        )
        await self._consumer.start()
        self._running = True
        self._task = asyncio.create_task(
            self._consume_loop(), name="kafka-consumer"
        )
        logger.info(
            "Kafka consumer started brokers=%s topics=%s group=%s",
            self._brokers,
            self._topics,
            self._group_id,
        )

    async def stop(self) -> None:
        """Gracefully stop the consumer."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None
        logger.info("Kafka consumer stopped")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _consume_loop(self) -> None:
        """Main polling loop."""
        assert self._consumer is not None
        while self._running:
            try:
                async for msg in self._consumer:
                    if not self._running:
                        break
                    await self._process_message(msg.value)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Kafka consumer error: %s – restarting in 5s", exc)
                await asyncio.sleep(5)

    async def _process_message(self, raw: bytes) -> None:
        """Deserialise and dispatch a single Kafka message."""
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.warning("Invalid Kafka message (JSON decode error): %s", exc)
            return

        try:
            # SpanBatch: {"spans": [...]}
            if isinstance(data, dict) and "spans" in data:
                batch = SpanBatch.model_validate(data)
                for span in batch.spans:
                    await self._handler.handle(span)
            # Single span object
            elif isinstance(data, dict):
                span = SpanEvent.model_validate(data)
                await self._handler.handle(span)
            # Array of spans (alternative wire format)
            elif isinstance(data, list):
                batch = SpanBatch(spans=[SpanEvent.model_validate(s) for s in data])
                for span in batch.spans:
                    await self._handler.handle(span)
            else:
                logger.warning("Unrecognised Kafka message shape: %s", type(data))
        except Exception as exc:
            logger.error("Failed to process Kafka message: %s", exc)
