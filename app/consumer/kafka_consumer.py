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

from ..config import settings
from ..engine.prom_metrics import kafka_consumer_lag, kafka_message_errors, kafka_messages_processed
from ..models.deployment import DeploymentEvent
from ..models.log import LogEvent
from ..models.metric import MetricEvent
from ..models.span import SpanBatch, SpanEvent
from .deploy_event_handler import DeployEventHandler
from .event_handler import EventHandler
from .log_event_handler import LogEventHandler
from .metric_event_handler import MetricEventHandler

logger = logging.getLogger(__name__)

_POLL_TIMEOUT_MS = 1000
_MAX_BATCH_SIZE = 500
_LAG_REPORT_INTERVAL_SECONDS = 30
_SUPPORTED_SCHEMA_VERSION = "1"


def _check_schema_version(data: dict, topic_type: str) -> None:
    """Log a warning if the message carries an unrecognised schema version."""
    version = data.get("schema_version")
    if version is not None and version != _SUPPORTED_SCHEMA_VERSION:
        logger.warning(
            "Unsupported schema_version=%s topic_type=%s — expected %s. "
            "Rolling upgrade in progress or schema mismatch.",
            version,
            topic_type,
            _SUPPORTED_SCHEMA_VERSION,
        )


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
        metric_handler: MetricEventHandler | None = None,
        log_handler: LogEventHandler | None = None,
        deploy_handler: DeployEventHandler | None = None,
        brokers: str | None = None,
        topics: str | None = None,
        metrics_topics: str | None = None,
        log_topics: str | None = None,
        deploy_topics: str | None = None,
        group_id: str | None = None,
    ) -> None:
        self._handler = event_handler
        self._metric_handler = metric_handler
        self._log_handler = log_handler
        self._deploy_handler = deploy_handler
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
        deploy_topics_list = [
            t.strip()
            for t in (deploy_topics or settings.KAFKA_DEPLOY_TOPICS).split(",")
            if t.strip()
        ]

        self._span_topics: set[str] = set(span_topics)
        self._metric_topics: set[str] = set(metric_topics_list)
        self._log_topics: set[str] = set(log_topics_list)
        self._deploy_topics: set[str] = set(deploy_topics_list)
        # deduplicate while preserving order: spans, then metrics, then logs, then deploys
        seen: set[str] = set(span_topics)
        extra_metrics = [t for t in metric_topics_list if t not in seen]
        seen.update(extra_metrics)
        extra_logs = [t for t in log_topics_list if t not in seen]
        seen.update(extra_logs)
        extra_deploys = [t for t in deploy_topics_list if t not in seen]
        self._all_topics = span_topics + extra_metrics + extra_logs + extra_deploys

        self._group_id = group_id or settings.KAFKA_GROUP_ID
        self._consumer = None
        self._task: asyncio.Task | None = None
        self._lag_task: asyncio.Task | None = None
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
            enable_auto_commit=False,          # manual commit after each dispatch
            max_poll_records=_MAX_BATCH_SIZE,
            session_timeout_ms=30000,
            heartbeat_interval_ms=3000,
        )
        await self._consumer.start()
        self._running = True
        self._task = asyncio.create_task(
            self._consume_loop(), name="kafka-consumer"
        )
        self._lag_task = asyncio.create_task(
            self._lag_reporter_loop(), name="kafka-lag-reporter"
        )
        logger.info(
            "Kafka consumer started brokers=%s span_topics=%s metric_topics=%s log_topics=%s deploy_topics=%s group=%s",
            self._brokers,
            sorted(self._span_topics),
            sorted(self._metric_topics),
            sorted(self._log_topics),
            sorted(self._deploy_topics),
            self._group_id,
        )

    async def stop(self) -> None:
        """Gracefully stop the consumer."""
        self._running = False
        for task in (self._task, self._lag_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None
        logger.info("Kafka consumer stopped")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _lag_reporter_loop(self) -> None:
        """Periodically fetch end offsets from broker and log consumer lag."""
        while self._running:
            await asyncio.sleep(_LAG_REPORT_INTERVAL_SECONDS)
            if self._consumer is None:
                continue
            try:
                assignment = self._consumer.assignment()
                if not assignment:
                    continue
                end_offsets = await self._consumer.end_offsets(list(assignment))
                for tp, end_offset in end_offsets.items():
                    try:
                        position = await self._consumer.position(tp)
                        lag = max(0, end_offset - position)
                        kafka_consumer_lag.labels(
                            topic=tp.topic, partition=str(tp.partition)
                        ).set(lag)
                        if lag > 0:
                            logger.debug("Kafka lag topic=%s partition=%d lag=%d", tp.topic, tp.partition, lag)
                    except Exception:
                        pass
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.debug("Lag report failed: %s", exc)

    async def _consume_loop(self) -> None:
        """Main polling loop – dispatches by topic then commits offset."""
        assert self._consumer is not None
        while self._running:
            try:
                async for msg in self._consumer:
                    if not self._running:
                        break
                    if msg.topic in self._deploy_topics:
                        topic_type = "deploy"
                        await self._process_deploy_message(msg.value)
                    elif msg.topic in self._log_topics:
                        topic_type = "log"
                        await self._process_log_message(msg.value)
                    elif msg.topic in self._metric_topics:
                        topic_type = "metric"
                        await self._process_metric_message(msg.value)
                    else:
                        topic_type = "span"
                        await self._process_span_message(msg.value)
                    kafka_messages_processed.labels(topic_type=topic_type).inc()
                    # Commit offset only after successful dispatch (at-least-once).
                    # Malformed messages are still committed so we skip poison pills.
                    try:
                        await self._consumer.commit()
                    except Exception as commit_exc:
                        logger.warning("Kafka commit failed: %s", commit_exc)
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
            kafka_message_errors.labels(topic_type="span").inc()
            return

        try:
            if isinstance(data, dict) and "spans" in data:
                _check_schema_version(data, "span")
                batch = SpanBatch.model_validate(data)
                for span in batch.spans:
                    await self._handler.handle(span)
            elif isinstance(data, dict):
                _check_schema_version(data, "span")
                span = SpanEvent.model_validate(data)
                await self._handler.handle(span)
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        _check_schema_version(item, "span")
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
                _check_schema_version(data, "metric")
                event = MetricEvent.model_validate(data)
                await self._metric_handler.handle(event)
            else:
                logger.warning("Unrecognised metric Kafka message shape: %s", type(data))
        except Exception as exc:
            logger.error("Failed to process metric Kafka message: %s", exc)

    async def _process_deploy_message(self, raw: bytes) -> None:
        """Deserialise and dispatch a single deployment Kafka message."""
        if self._deploy_handler is None:
            return

        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.warning("Invalid deploy Kafka message (JSON decode error): %s", exc)
            return

        try:
            if isinstance(data, dict):
                _check_schema_version(data, "deploy")
                event = DeploymentEvent.model_validate(data)
                await self._deploy_handler.handle(event)
            else:
                logger.warning("Unrecognised deploy Kafka message shape: %s", type(data))
        except Exception as exc:
            logger.error("Failed to process deploy Kafka message: %s", exc)

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
                _check_schema_version(data, "log")
                event = LogEvent.model_validate(data)
                await self._log_handler.handle(event)
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        _check_schema_version(item, "log")
                    event = LogEvent.model_validate(item)
                    await self._log_handler.handle(event)
            else:
                logger.warning("Unrecognised log Kafka message shape: %s", type(data))
        except Exception as exc:
            logger.error("Failed to process log Kafka message: %s", exc)
