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
from .consumer.event_handler import EventHandler
from .consumer.kafka_consumer import KafkaConsumerService
from .consumer.metric_event_handler import MetricEventHandler
from .engine.burn_rate_analyzer import BurnRateAnalyzer
from .engine.dependency_map import DependencyMap
from .engine.feature_store import FeatureStore
from .engine.forecaster import Forecaster
from .engine.granger_analyzer import GrangerAnalyzer
from .engine.jvm_analyzer import JvmAnalyzer
from .engine.jvm_feature_store import JvmFeatureStore
from .engine.metric_feature_store import MetricFeatureStore
from .engine.var_forecaster import VarForecaster
from .store.clickhouse import ClickHouseStore
from .store.forecast_store import ForecastStore

# ---------------------------------------------------------------------------
# Logging configuration
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
    logger.info("javi-forecast starting up …")

    # ---- Build shared components ----------------------------------------
    feature_store = FeatureStore(maxlen=4320)              # 72 h at 1-min cadence
    jvm_feature_store = JvmFeatureStore(maxlen=4320)       # 72 h at 1-min cadence
    metric_feature_store = MetricFeatureStore(maxlen=4320) # 72 h at 1-min cadence
    forecast_store = ForecastStore(ttl_seconds=3600)
    event_handler = EventHandler(feature_store)
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
    forecaster = Forecaster(
        feature_store=feature_store,
        forecast_store=forecast_store,
        anomaly_predictor=anomaly_predictor,
        alerter=alerter,
        metric_feature_store=metric_feature_store,
    )

    # Expose shared state via app.state for dependency injection
    app.state.feature_store = feature_store
    app.state.jvm_feature_store = jvm_feature_store
    app.state.metric_feature_store = metric_feature_store
    app.state.forecast_store = forecast_store
    app.state.event_handler = event_handler
    app.state.metric_event_handler = metric_event_handler
    app.state.forecaster = forecaster
    app.state.jvm_analyzer = jvm_analyzer
    app.state.burn_rate_analyzer = burn_rate_analyzer
    app.state.dependency_map = dependency_map
    app.state.var_forecaster = var_forecaster
    app.state.granger_analyzer = granger_analyzer

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
        )
        try:
            await kafka.start()
        except Exception as exc:
            logger.error("Kafka consumer failed to start: %s", exc)
            kafka = None
    else:
        logger.info("Kafka consumer disabled (KAFKA_ENABLED=false)")

    app.state.kafka = kafka

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
    set_dependencies(clickhouse, feature_store, ready=True)
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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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
