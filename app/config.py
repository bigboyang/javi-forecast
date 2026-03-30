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
    KAFKA_TOPICS: str = "spans.all"   # comma-separated; spans.all includes all spans (accurate RED metrics)
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

    class Config:
        env_file = ".env"


settings = Settings()
