"""Log event handler: routes incoming OTel log events to the LogStore.

Called by KafkaConsumerService when a message arrives on a log topic,
and by the HTTP ingest endpoint (POST /v1/logs).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from ..models.log import LogEvent
from ..rag.log_store import LogStore

logger = logging.getLogger(__name__)

_logs_counter = None


def _get_counter():
    global _logs_counter
    if _logs_counter is None:
        try:
            from prometheus_client import Counter
            _logs_counter = Counter(
                "javi_forecast_logs_ingested_total",
                "Total OTel log records ingested",
                ["service", "severity"],
            )
        except Exception:
            pass
    return _logs_counter


class LogEventHandler:
    """Routes LogEvent to the LogStore.

    Parameters
    ----------
    log_store:
        Shared LogStore instance.
    """

    def __init__(self, log_store: LogStore) -> None:
        self._store = log_store

    async def handle(self, event: LogEvent) -> None:
        """Process a single LogEvent.

        Parameters
        ----------
        event:
            The log event to ingest.
        """
        try:
            ts = datetime.fromtimestamp(
                event.timestamp_ms / 1000.0, tz=timezone.utc
            )
            self._store.add_log(
                service_name=event.service_name,
                severity=event.severity,
                body=event.body,
                timestamp=ts,
                trace_id=event.trace_id,
                span_id=event.span_id,
            )
            counter = _get_counter()
            if counter is not None:
                counter.labels(
                    service=event.service_name,
                    severity=event.severity,
                ).inc()
            logger.debug(
                "log ingested service=%s severity=%s body_len=%d",
                event.service_name,
                event.severity,
                len(event.body),
            )
        except Exception as exc:
            logger.error(
                "failed to handle log service=%s: %s",
                event.service_name,
                exc,
            )
