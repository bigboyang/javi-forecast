"""Webhook / Slack alerter with per-service rate limiting.

Fires an HTTP POST to ``ALERT_WEBHOOK_URL`` (generic) and / or
``ALERT_SLACK_WEBHOOK_URL`` (Slack) whenever a new anomaly is predicted.

Rate limiting
-------------
At most one alert per service per metric per *cooldown* window
(default 5 minutes) to avoid alert storms.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Tuple

import httpx

from ..anomaly.predictor import AnomalyPrediction
from ..config import settings

logger = logging.getLogger(__name__)

_DEFAULT_COOLDOWN_SECONDS = 300   # 5 minutes


class WebhookAlerter:
    """Async alerter that dispatches anomaly notifications.

    Parameters
    ----------
    cooldown_seconds:
        Minimum time between consecutive alerts for the same
        (service, metric) pair.
    """

    def __init__(self, cooldown_seconds: int = _DEFAULT_COOLDOWN_SECONDS) -> None:
        self.cooldown_seconds = cooldown_seconds
        self._last_fired: Dict[Tuple[str, str], datetime] = {}
        self._lock = asyncio.Lock()
        self._client: Optional[httpx.AsyncClient] = None

    async def start(self) -> None:
        self._client = httpx.AsyncClient(timeout=10.0)

    async def stop(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fire(self, prediction: AnomalyPrediction) -> None:
        """Fire alerts for *prediction* (rate-limited)."""
        key = (prediction.service_name, prediction.metric_name)
        async with self._lock:
            last = self._last_fired.get(key)
            now = datetime.now(tz=timezone.utc)
            if last is not None:
                elapsed = (now - last).total_seconds()
                if elapsed < self.cooldown_seconds:
                    logger.debug(
                        "alert suppressed (cooldown) service=%s metric=%s elapsed=%.0fs",
                        prediction.service_name,
                        prediction.metric_name,
                        elapsed,
                    )
                    return
            self._last_fired[key] = now

        payload = self._build_payload(prediction)
        tasks = []

        if settings.ALERT_WEBHOOK_URL:
            tasks.append(
                self._post(settings.ALERT_WEBHOOK_URL, payload, label="generic")
            )
        if settings.ALERT_SLACK_WEBHOOK_URL:
            slack_payload = self._build_slack_payload(prediction)
            tasks.append(
                self._post(
                    settings.ALERT_SLACK_WEBHOOK_URL,
                    slack_payload,
                    label="slack",
                )
            )

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, Exception):
                    logger.error("alert delivery error: %s", r)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _post(
        self, url: str, payload: dict, *, label: str = ""
    ) -> None:
        client = self._client
        if client is None:
            logger.error("alerter client not started")
            return
        try:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            logger.info(
                "alert sent target=%s service=%s status=%d",
                label,
                payload.get("service_name"),
                resp.status_code,
            )
        except httpx.HTTPStatusError as exc:
            logger.error(
                "alert HTTP error target=%s status=%d body=%s",
                label,
                exc.response.status_code,
                exc.response.text[:200],
            )
        except Exception as exc:
            logger.error("alert error target=%s: %s", label, exc)

    @staticmethod
    def _build_payload(prediction: AnomalyPrediction) -> dict:
        return {
            "service_name": prediction.service_name,
            "metric_name": prediction.metric_name,
            "severity": prediction.severity,
            "predicted_value": prediction.predicted_value,
            "expected_value": prediction.expected_value,
            "z_score": round(prediction.z_score, 3),
            "predicted_at": prediction.predicted_at.isoformat(),
            "detected_at": prediction.detected_at.isoformat(),
            "seconds_until_anomaly": round(prediction.seconds_until_anomaly, 1),
            "source": "javi-forecast",
        }

    @staticmethod
    def _build_slack_payload(prediction: AnomalyPrediction) -> dict:
        icon = ":rotating_light:" if prediction.severity == "critical" else ":warning:"
        text = (
            f"{icon} *javi-forecast anomaly alert*\n"
            f"*Service:* `{prediction.service_name}`\n"
            f"*Metric:* `{prediction.metric_name}`\n"
            f"*Severity:* `{prediction.severity.upper()}`\n"
            f"*Predicted value:* `{prediction.predicted_value:.2f}` "
            f"(expected ~`{prediction.expected_value:.2f}`, z={prediction.z_score:.2f})\n"
            f"*Expected at:* `{prediction.predicted_at.isoformat()}`\n"
            f"*Time until anomaly:* `{prediction.seconds_until_anomaly:.0f}s`"
        )
        return {"text": text}

    # ------------------------------------------------------------------
    # Real-time anomaly alerts (from AnomalyDetector, not Forecaster)
    # ------------------------------------------------------------------

    async def fire_anomaly(self, anomaly: dict) -> None:
        """Fire alerts for a *current* anomaly detected by AnomalyDetector.

        Parameters
        ----------
        anomaly:
            dict with keys: service_name, span_name, anomaly_type,
            current_value, baseline_value, z_score, severity, minute
        """
        svc = anomaly.get("service_name", "")
        atype = anomaly.get("anomaly_type", "anomaly")
        key = (svc, atype)

        async with self._lock:
            last = self._last_fired.get(key)
            now = datetime.now(tz=timezone.utc)
            if last is not None and (now - last).total_seconds() < self.cooldown_seconds:
                return
            self._last_fired[key] = now

        payload = {
            "service_name": svc,
            "span_name": anomaly.get("span_name", ""),
            "anomaly_type": atype,
            "severity": anomaly.get("severity", "warning"),
            "current_value": anomaly.get("current_value", 0),
            "baseline_value": anomaly.get("baseline_value", 0),
            "z_score": round(float(anomaly.get("z_score", 0)), 3),
            "minute": anomaly.get("minute", now).isoformat()
            if hasattr(anomaly.get("minute"), "isoformat")
            else str(anomaly.get("minute", "")),
            "source": "javi-forecast/anomaly-detector",
        }
        tasks = []
        if settings.ALERT_WEBHOOK_URL:
            tasks.append(self._post(settings.ALERT_WEBHOOK_URL, payload, label="generic"))
        if settings.ALERT_SLACK_WEBHOOK_URL:
            sev = anomaly.get("severity", "warning")
            icon = ":rotating_light:" if sev == "critical" else ":warning:"
            slack_payload = {
                "text": (
                    f"{icon} *[javi-forecast] 이상 탐지*\n"
                    f"*Service:* `{svc}` / `{anomaly.get('span_name', '')}`\n"
                    f"*Type:* `{atype}`  *Severity:* `{sev.upper()}`\n"
                    f"*Current:* `{payload['current_value']:.3f}`  "
                    f"*Baseline:* `{payload['baseline_value']:.3f}`  "
                    f"*Z:* `{payload['z_score']}`"
                )
            }
            tasks.append(
                self._post(settings.ALERT_SLACK_WEBHOOK_URL, slack_payload, label="slack")
            )

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, Exception):
                    logger.error("anomaly alert delivery error: %s", r)
