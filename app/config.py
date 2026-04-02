from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # Server
    HTTP_PORT: int = 8080
    LOG_LEVEL: str = "info"

    # ClickHouse
    CLICKHOUSE_HOST: str = "localhost"
    CLICKHOUSE_PORT: int = 9000
    CLICKHOUSE_DB: str = "apm"
    CLICKHOUSE_USER: str = "default"
    CLICKHOUSE_PASSWORD: str = ""
    DISABLE_CLICKHOUSE: bool = False

    # Kafka
    KAFKA_ENABLED: bool = False
    KAFKA_BROKERS: str = "localhost:9092"
    KAFKA_TOPICS: str = "spans.all"         # comma-separated span topics; spans.all = accurate RED metrics
    KAFKA_METRICS_TOPICS: str = "metrics"   # comma-separated OTel metric topics from javi-collector
    KAFKA_GROUP_ID: str = "javi-forecast"

    # Forecasting
    FORECAST_HORIZON_MINUTES: int = 30       # predict N minutes ahead
    FORECAST_INTERVAL_SECONDS: int = 60      # run forecast every N seconds
    FEATURE_WINDOW_MINUTES: int = 60         # lookback window for features
    MIN_DATA_POINTS: int = 10                # minimum points before forecasting

    # Model selection
    DEFAULT_MODEL: str = "ewma"              # ewma | arima | holtwinters | auto

    # Alerting
    ALERT_WEBHOOK_URL: Optional[str] = None
    ALERT_SLACK_WEBHOOK_URL: Optional[str] = None
    ALERT_FORECAST_THRESHOLD: float = 2.0    # Z-score threshold for forecast alerts

    # Backfill
    BACKFILL_ENABLED: bool = True
    BACKFILL_HOURS: int = 24

    # JVM Analysis
    JVM_OOM_WARN_MINUTES: float = 30.0      # alert if OOM predicted within N minutes
    JVM_GC_OVERHEAD_WARN: float = 0.10      # alert if GC overhead > N fraction

    # P2-A: Hour-of-week baseline
    BASELINE_MIN_OBSERVATIONS: int = 5      # min bucket obs before baseline is used

    # P2-B: SLO burn rate alerting
    SLO_TARGET: float = 0.999               # 99.9 % availability target
    SLO_PAGE_BURN_RATE: float = 14.4        # critical if fast+slow burn > this
    SLO_TICKET_BURN_RATE: float = 6.0       # warn if fast+slow burn > this

    # P2-C: Isolation Forest
    ISO_FOREST_MIN_SAMPLES: int = 50        # min RED vectors before fitting

    # P3-A: VAR cross-service forecaster
    VAR_ENABLED: bool = True

    # P3-B/C: Granger causality + dependency map
    GRANGER_ENABLED: bool = True
    GRANGER_INTERVAL_SECONDS: int = 600     # run Granger tests every 10 minutes
    GRANGER_P_THRESHOLD: float = 0.05      # significance level for causal edges

    class Config:
        env_file = ".env"


settings = Settings()
