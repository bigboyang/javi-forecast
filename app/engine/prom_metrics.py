"""Centralised Prometheus metric registry.

Import specific metrics from here rather than creating scattered metric objects.
All labels are kept low-cardinality — service_name is intentionally excluded.
"""
from prometheus_client import Counter, Gauge, Histogram

# Kafka ingestion
kafka_messages_processed = Counter(
    "javi_kafka_messages_processed_total",
    "Kafka messages dispatched to handlers",
    ["topic_type"],  # span | metric | log | deploy
)

kafka_message_errors = Counter(
    "javi_kafka_message_errors_total",
    "Kafka messages that failed JSON decode or schema validation",
    ["topic_type"],
)

# Anomaly detection
anomalies_detected = Counter(
    "javi_anomalies_detected_total",
    "Anomalies detected per cycle",
    ["anomaly_type", "severity"],
)

anomalies_suppressed = Counter(
    "javi_anomalies_suppressed_total",
    "Anomaly events suppressed by the deduplication window",
)

# RCA
rca_reports_generated = Counter(
    "javi_rca_reports_generated_total",
    "RCA reports written to ClickHouse",
)

# Forecast
forecast_cycles = Counter(
    "javi_forecast_cycles_total",
    "Completed forecast cycles",
    ["status"],  # ok | error
)

forecast_cycle_duration = Histogram(
    "javi_forecast_cycle_duration_seconds",
    "Wall-clock time for one full forecast cycle",
    buckets=[0.5, 1, 2, 5, 10, 30, 60],
)

# FeatureStore cardinality
feature_store_services = Gauge(
    "javi_feature_store_services",
    "Live services currently tracked in the in-memory FeatureStore",
)

feature_store_evictions = Counter(
    "javi_feature_store_evictions_total",
    "LRU evictions triggered by MAX_FEATURE_STORE_SERVICES limit",
)

# Alert lifecycle
alerts_fired = Counter(
    "javi_alerts_fired_total",
    "Alert webhooks dispatched",
    ["severity"],
)

alerts_active = Gauge(
    "javi_alerts_active",
    "Alerts currently in FIRING state",
)
