"""javi-forecast: APM Forecasting Service.

FastAPI application with lifespan management.

Startup sequence
----------------
1. Initialise ClickHouse client and verify connectivity.
2. Backfill the FeatureStore from ClickHouse history (if enabled).
3. Start the Kafka consumer (if enabled).
4. Start the background Forecaster loop.
5. Mark service as ready.

Shutdown sequence
-----------------
1. Stop Forecaster.
2. Stop Kafka consumer.
3. Flush open feature-store buckets.
4. Close ClickHouse connection.
"""

from __future__ import annotations

import logging
import sys
from contextlib import asynccontextmanager
from typing import AsyncIterator

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api.router import api_router
from .api.health import set_dependencies, mark_ready
from .anomaly.predictor import AnomalyPredictor
from .alerter.webhook import WebhookAlerter
from .config import settings
from .consumer.deploy_event_handler import DeployEventHandler
from .consumer.event_handler import EventHandler
from .consumer.kafka_consumer import KafkaConsumerService
from .consumer.log_event_handler import LogEventHandler
from .consumer.metric_event_handler import MetricEventHandler
from .engine.burn_rate_analyzer import BurnRateAnalyzer
from .engine.dependency_map import DependencyMap
from .engine.deployment_store import DeploymentStore
from .engine.feature_store import FeatureStore
from .engine.service_registry import ServiceRegistry
from .engine.feedback_store import FeedbackStore as AlertFeedbackStore
from .engine.forecaster import Forecaster
from .engine.granger_analyzer import GrangerAnalyzer
from .engine.jvm_analyzer import JvmAnalyzer
from .engine.jvm_feature_store import JvmFeatureStore
from .engine.metric_feature_store import MetricFeatureStore
from .engine.span_topology import SpanTopologyTracker
from .engine.var_forecaster import VarForecaster
from .engine.accuracy_tracker import AccuracyTracker
from .engine.alert_store import AlertStore
from .engine.anomaly_clusterer import AnomalyClusterer
from .engine.baseline_computer import BaselineComputer
from .engine.anomaly_detector import AnomalyDetector
from .engine.rca_engine import RCAEngine
from .store.clickhouse import ClickHouseStore
from .store.forecast_store import ForecastStore

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Instance identity metric
# ---------------------------------------------------------------------------


# kafka 구독 추가, 메시지 처리 로직 추가

logging.basicConfig(
    stream=sys.stdout,
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("javi_forecast")


# ---------------------------------------------------------------------------
# Application lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage startup and shutdown of background services."""
    logger.info("javi-forecast starting up … instance_id=%s", settings.INSTANCE_ID)

    # ---- Redis (optional, for HA feature-store sharing) -----------------
    redis_client = None
    if settings.REDIS_URL:
        try:
            import redis.asyncio as aioredis
            redis_client = aioredis.from_url(
                settings.REDIS_URL,
                encoding="utf-8",
                decode_responses=False,
                socket_connect_timeout=5,
            )
            await redis_client.ping()
            logger.info("Redis connected: %s", settings.REDIS_URL)
        except Exception as exc:
            logger.warning("Redis connection failed: %s – FeatureStore will be in-memory only", exc)
            redis_client = None
    else:
        logger.warning(
            "REDIS_URL is not set – FeatureStore is in-memory only. "
            "Running multiple replicas will result in split-brain forecasts. "
            "Set REDIS_URL and INSTANCE_ID for HA deployments."
        )

    # ---- Build shared components ----------------------------------------
    feature_store = FeatureStore(
        maxlen=4320,
        max_services=settings.MAX_FEATURE_STORE_SERVICES,
        redis_client=redis_client,
    )  # 72 h at 1-min cadence
    jvm_feature_store = JvmFeatureStore(maxlen=4320)       # 72 h at 1-min cadence
    metric_feature_store = MetricFeatureStore(maxlen=4320) # 72 h at 1-min cadence
    forecast_store = ForecastStore(ttl_seconds=3600)
    span_topology = SpanTopologyTracker()
    feedback_store = AlertFeedbackStore(ttl_seconds=7 * 86400)
    alert_store = AlertStore(ttl_hours=24)  # clickhouse wired up below after CH connect
    anomaly_clusterer = AnomalyClusterer(window_seconds=300)
    accuracy_tracker = AccuracyTracker(maxlen=500)
    service_registry = ServiceRegistry()
    deployment_store = DeploymentStore()
    deploy_event_handler = DeployEventHandler(deployment_store)
    event_handler = EventHandler(
        feature_store,
        topology_tracker=span_topology,
        service_registry=service_registry,
    )
    metric_event_handler = MetricEventHandler(metric_feature_store)
    anomaly_predictor = AnomalyPredictor(
        warn_z=settings.ALERT_FORECAST_THRESHOLD,
        critical_z=settings.ALERT_FORECAST_THRESHOLD + 1.0,
    )
    alerter = WebhookAlerter(cooldown_seconds=300)
    jvm_analyzer = JvmAnalyzer(
        jvm_store=jvm_feature_store,
        alerter=alerter,
    )
    burn_rate_analyzer = BurnRateAnalyzer(
        feature_store=feature_store,
        alerter=alerter,
    )
    dependency_map = DependencyMap(p_value_threshold=settings.GRANGER_P_THRESHOLD)
    var_forecaster = VarForecaster(
        feature_store=feature_store,
        forecast_store=forecast_store,
    )
    granger_analyzer = GrangerAnalyzer(
        feature_store=feature_store,
        dependency_map=dependency_map,
    )
    # ---- Incident store (Phase 2 RAG) ---------------------------------------
    incident_store = None
    if settings.RAG_ENABLED and settings.INCIDENT_RAG_ENABLED:
        from .rag.incident_store import IncidentStore
        incident_store = IncidentStore(persist_directory=settings.INCIDENT_STORE_PATH)
        logger.info("IncidentStore initialised at %s", settings.INCIDENT_STORE_PATH)

    app.state.incident_store = incident_store

    # ---- Log store (Phase 3 RAG) --------------------------------------------
    log_store = None
    log_event_handler: LogEventHandler | None = None
    if settings.RAG_ENABLED and settings.LOG_RAG_ENABLED:
        from .rag.log_store import LogStore
        log_store = LogStore(persist_directory=settings.LOG_STORE_PATH)
        log_event_handler = LogEventHandler(log_store)
        logger.info("LogStore initialised at %s", settings.LOG_STORE_PATH)
    else:
        logger.info("Log RAG disabled (LOG_RAG_ENABLED=false or RAG_ENABLED=false)")

    app.state.log_store = log_store

    forecaster = Forecaster(
        feature_store=feature_store,
        forecast_store=forecast_store,
        anomaly_predictor=anomaly_predictor,
        alerter=alerter,
        metric_feature_store=metric_feature_store,
        incident_store=incident_store,
        accuracy_tracker=accuracy_tracker,
    )

    # Expose shared state via app.state for dependency injection
    app.state.feature_store = feature_store
    app.state.jvm_feature_store = jvm_feature_store
    app.state.metric_feature_store = metric_feature_store
    app.state.deployment_store = deployment_store
    app.state.deploy_event_handler = deploy_event_handler
    app.state.forecast_store = forecast_store
    app.state.event_handler = event_handler
    app.state.metric_event_handler = metric_event_handler
    app.state.forecaster = forecaster
    app.state.jvm_analyzer = jvm_analyzer
    app.state.burn_rate_analyzer = burn_rate_analyzer
    app.state.dependency_map = dependency_map
    app.state.var_forecaster = var_forecaster
    app.state.granger_analyzer = granger_analyzer
    app.state.span_topology = span_topology
    app.state.feedback_store = feedback_store
    app.state.service_registry = service_registry
    app.state.alert_store = alert_store
    app.state.anomaly_clusterer = anomaly_clusterer
    app.state.accuracy_tracker = accuracy_tracker

    # ---- ClickHouse ---------------------------------------------------------
    clickhouse: ClickHouseStore | None = None
    if not settings.DISABLE_CLICKHOUSE:
        clickhouse = ClickHouseStore()
        try:
            await clickhouse.connect()
            ok = await clickhouse.ping()
            if not ok:
                logger.warning("ClickHouse ping failed – continuing without it")
                clickhouse = None
            else:
                logger.info("ClickHouse connection established")
        except Exception as exc:
            logger.error("ClickHouse connection error: %s – continuing without it", exc)
            clickhouse = None
    else:
        logger.info("ClickHouse disabled (DISABLE_CLICKHOUSE=true)")

    app.state.clickhouse = clickhouse

    # ---- Wire AlertStore ClickHouse + ensure table --------------------------
    if clickhouse is not None:
        alert_store._ch = clickhouse
        try:
            await clickhouse.ensure_alerts_table()
            n_alerts = await alert_store.load_from_clickhouse()
            logger.info("AlertStore: preloaded %d active alerts from ClickHouse", n_alerts)
        except Exception as exc:
            logger.warning("AlertStore: ClickHouse init failed: %s", exc)

    # ---- Redis preload (before backfill so ts_index deduplicates correctly) ----
    if redis_client is not None:
        try:
            loaded = await feature_store.load_from_redis()
            logger.info("Redis preload complete: %d points loaded", loaded)
        except Exception as exc:
            logger.warning("Redis preload failed: %s – starting from empty store", exc)

    # ---- Backfill -----------------------------------------------------------
    if clickhouse is not None and settings.BACKFILL_ENABLED:
        try:
            logger.info("Starting ClickHouse backfill …")
            await clickhouse.backfill_feature_store(feature_store)
        except Exception as exc:
            logger.error("Backfill failed: %s", exc)

    # ---- Alerter ------------------------------------------------------------
    await alerter.start()

    # ---- Kafka consumer (span + metric) ------------------------------------
    kafka: KafkaConsumerService | None = None
    if settings.KAFKA_ENABLED:
        kafka = KafkaConsumerService(
            event_handler=event_handler,
            metric_handler=metric_event_handler,
            log_handler=log_event_handler,
            deploy_handler=deploy_event_handler,
        )
        try:
            await kafka.start()
        except Exception as exc:
            logger.error("Kafka consumer failed to start: %s", exc)
            kafka = None
    else:
        logger.info("Kafka consumer disabled (KAFKA_ENABLED=false)")

    app.state.kafka = kafka

    # ---- Baseline Computer (ported from collector) --------------------------
    baseline_computer: BaselineComputer | None = None
    if clickhouse is not None and settings.BASELINE_ENABLED:
        baseline_computer = BaselineComputer(
            clickhouse=clickhouse,
            interval_hours=settings.BASELINE_INTERVAL_HOURS,
        )
        await baseline_computer.start()

    # ---- Anomaly Detector (ported from collector) ---------------------------
    anomaly_detector: AnomalyDetector | None = None
    if clickhouse is not None and settings.ANOMALY_ENABLED:
        anomaly_detector = AnomalyDetector(
            clickhouse=clickhouse,
            alerter=alerter,
            alert_store=alert_store,
            anomaly_clusterer=anomaly_clusterer,
        )
        await anomaly_detector.start()

    # ---- RCA Engine (ported from collector) ---------------------------------
    rca_engine: RCAEngine | None = None
    if clickhouse is not None and settings.RCA_ENABLED:
        rca_engine = RCAEngine(clickhouse=clickhouse, incident_store=incident_store)
        await rca_engine.start()

    # ---- JVM Analyzer -------------------------------------------------------
    await jvm_analyzer.start()

    # ---- Burn Rate Analyzer -------------------------------------------------
    await burn_rate_analyzer.start()

    # ---- VAR Forecaster (P3-A) ----------------------------------------------
    if settings.VAR_ENABLED:
        await var_forecaster.start()
    else:
        logger.info("VarForecaster disabled (VAR_ENABLED=false)")

    # ---- Granger Analyzer (P3-B/C) ------------------------------------------
    if settings.GRANGER_ENABLED:
        await granger_analyzer.start()
    else:
        logger.info("GrangerAnalyzer disabled (GRANGER_ENABLED=false)")

    # ---- Forecaster ---------------------------------------------------------
    await forecaster.start()

    # ---- Mark ready ---------------------------------------------------------
    set_dependencies(
        clickhouse,
        feature_store,
        ready=True,
        kafka_service=kafka,
        forecaster=forecaster,
        anomaly_detector=anomaly_detector,
    )
    mark_ready(True)
    logger.info("javi-forecast ready on port %d", settings.HTTP_PORT)

    # ---- Yield (server is running) -----------------------------------------
    yield

    # ---- Shutdown -----------------------------------------------------------
    logger.info("javi-forecast shutting down …")
    mark_ready(False)

    await forecaster.stop()
    await jvm_analyzer.stop()
    await burn_rate_analyzer.stop()
    if rca_engine is not None:
        await rca_engine.stop()
    if anomaly_detector is not None:
        await anomaly_detector.stop()
    if baseline_computer is not None:
        await baseline_computer.stop()
    if settings.VAR_ENABLED:
        await var_forecaster.stop()
    if settings.GRANGER_ENABLED:
        await granger_analyzer.stop()

    if kafka is not None:
        await kafka.stop()

    await feature_store.flush_open_buckets()
    await alerter.stop()

    if clickhouse is not None:
        await clickhouse.close()

    if redis_client is not None:
        await redis_client.aclose()

    logger.info("javi-forecast shutdown complete")


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="javi-forecast",
    description=(
        "APM Forecasting Service – consumes OTel spans from javi-collector, "
        "maintains a RED metrics feature store, and produces time-series "
        "forecasts with anomaly detection."
    ),
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

_cors_origins = [o.strip() for o in settings.CORS_ALLOWED_ORIGINS.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=settings.HTTP_PORT,
        log_level=settings.LOG_LEVEL.lower(),
        access_log=True,
    )
