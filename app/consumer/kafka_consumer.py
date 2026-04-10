"""Async Kafka consumer service.

Consumes JSON-encoded messages from span, metric, and log topics published by
javi-collector and forwards each event to the appropriate handler.

Span topics  → :class:`EventHandler`        (SpanEvent / SpanBatch)
Metric topics → :class:`MetricEventHandler` (MetricEvent)
Log topics   → :class:`LogEventHandler`     (LogEvent)

Message format – spans
----------------------
Each message value is a JSON object that can be deserialised into a
:class:`~app.models.span.SpanEvent` (single span) OR a
:class:`~app.models.span.SpanBatch` (list under ``"spans"`` key).

Message format – metrics
------------------------
Each message value is a JSON object matching
:class:`~app.models.metric.MetricEvent` (service_name, metric_name,
metric_type, value, timestamp_ms, …).

Message format – logs
---------------------
Each message value is a JSON object matching
:class:`~app.models.log.LogEvent` (service_name, severity, body,
timestamp_ms, …).
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional, Set

from ..config import settings
from ..models.log import LogEvent
from ..models.metric import MetricEvent
from ..models.span import SpanBatch, SpanEvent
from .event_handler import EventHandler
from .log_event_handler import LogEventHandler
from .metric_event_handler import MetricEventHandler

logger = logging.getLogger(__name__)

_POLL_TIMEOUT_MS = 1000
_MAX_BATCH_SIZE = 500


class KafkaConsumerService:
    """Manages the aiokafka consumer lifecycle.

    Parameters
    ----------
    event_handler:
        Handler that processes individual span events.
    metric_handler:
        Handler that processes individual metric events.  When *None*
        metric messages are silently discarded.
    brokers:
        Comma-separated Kafka broker addresses.
    topics:
        Comma-separated list of Kafka **span** topics to subscribe to.
        Defaults to ``KAFKA_TOPICS`` setting (e.g. ``"spans.all"``).
        Use ``spans.all`` for accurate RED metrics (rate + latency + errors).
    metrics_topics:
        Comma-separated list of Kafka **metric** topics to subscribe to.
        Defaults to ``KAFKA_METRICS_TOPICS`` setting (e.g. ``"metrics"``).
    group_id:
        Consumer group ID.
    """

    def __init__(
        self,
        event_handler: EventHandler,
        metric_handler: Optional[MetricEventHandler] = None,
        log_handler: Optional[LogEventHandler] = None,
        brokers: Optional[str] = None,
        topics: Optional[str] = None,
        metrics_topics: Optional[str] = None,
        log_topics: Optional[str] = None,
        group_id: Optional[str] = None,
    ) -> None:
        self._handler = event_handler
        self._metric_handler = metric_handler
        self._log_handler = log_handler
        self._brokers = brokers or settings.KAFKA_BROKERS

        span_topics = [
            t.strip()
            for t in (topics or settings.KAFKA_TOPICS).split(",")
            if t.strip()
        ]
        metric_topics_list = [
            t.strip()
            for t in (metrics_topics or settings.KAFKA_METRICS_TOPICS).split(",")
            if t.strip()
        ]
        log_topics_list = [
            t.strip()
            for t in (log_topics or settings.KAFKA_LOG_TOPICS).split(",")
            if t.strip()
        ]

        self._span_topics: Set[str] = set(span_topics)
        self._metric_topics: Set[str] = set(metric_topics_list)
        self._log_topics: Set[str] = set(log_topics_list)
        # deduplicate while preserving order: spans, then metrics, then logs
        seen: Set[str] = set(span_topics)
        extra_metrics = [t for t in metric_topics_list if t not in seen]
        seen.update(extra_metrics)
        extra_logs = [t for t in log_topics_list if t not in seen]
        self._all_topics = span_topics + extra_metrics + extra_logs

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
            *self._all_topics,
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
            "Kafka consumer started brokers=%s span_topics=%s metric_topics=%s log_topics=%s group=%s",
            self._brokers,
            sorted(self._span_topics),
            sorted(self._metric_topics),
            sorted(self._log_topics),
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
        """Main polling loop – dispatches by topic."""
        assert self._consumer is not None
        while self._running:
            try:
                async for msg in self._consumer:
                    if not self._running:
                        break
                    if msg.topic in self._log_topics:
                        await self._process_log_message(msg.value)
                    elif msg.topic in self._metric_topics:
                        await self._process_metric_message(msg.value)
                    else:
                        await self._process_span_message(msg.value)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Kafka consumer error: %s – restarting in 5s", exc)
                await asyncio.sleep(5)

    async def _process_span_message(self, raw: bytes) -> None:
        """Deserialise and dispatch a single span Kafka message."""
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.warning("Invalid span Kafka message (JSON decode error): %s", exc)
            return

        try:
            if isinstance(data, dict) and "spans" in data:
                batch = SpanBatch.model_validate(data)
                for span in batch.spans:
                    await self._handler.handle(span)
            elif isinstance(data, dict):
                span = SpanEvent.model_validate(data)
                await self._handler.handle(span)
            elif isinstance(data, list):
                for item in data:
                    span = SpanEvent.model_validate(item)
                    await self._handler.handle(span)
            else:
                logger.warning("Unrecognised span Kafka message shape: %s", type(data))
        except Exception as exc:
            logger.error("Failed to process span Kafka message: %s", exc)

    async def _process_metric_message(self, raw: bytes) -> None:
        """Deserialise and dispatch a single metric Kafka message."""
        if self._metric_handler is None:
            return

        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.warning("Invalid metric Kafka message (JSON decode error): %s", exc)
            return

        try:
            if isinstance(data, dict):
                event = MetricEvent.model_validate(data)
                await self._metric_handler.handle(event)
            else:
                logger.warning("Unrecognised metric Kafka message shape: %s", type(data))
        except Exception as exc:
            logger.error("Failed to process metric Kafka message: %s", exc)

    async def _process_log_message(self, raw: bytes) -> None:
        """Deserialise and dispatch a single log Kafka message."""
        if self._log_handler is None:
            return

        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.warning("Invalid log Kafka message (JSON decode error): %s", exc)
            return

        try:
            if isinstance(data, dict):
                event = LogEvent.model_validate(data)
                await self._log_handler.handle(event)
            elif isinstance(data, list):
                for item in data:
                    event = LogEvent.model_validate(item)
                    await self._log_handler.handle(event)
            else:
                logger.warning("Unrecognised log Kafka message shape: %s", type(data))
        except Exception as exc:
            logger.error("Failed to process log Kafka message: %s", exc)
