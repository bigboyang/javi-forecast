# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install (editable + dev deps)
pip install -e ".[dev]"

# Dev server (no ClickHouse, no Kafka)
make dev
# Equivalent: DISABLE_CLICKHOUSE=true KAFKA_ENABLED=false LOG_LEVEL=debug uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload

# Full stack (requires docker compose up first)
make run

# Docker stack
make docker-up   # builds image, starts ClickHouse + Kafka + javi-forecast
make docker-down

# Lint
make lint        # ruff check app/ && mypy app/ --ignore-missing-imports

# Format
make fmt         # ruff format app/

# Tests
make test                        # all tests
pytest tests/test_rca_engine.py  # single test file
pytest tests/ -v -k "test_name"  # single test by name
make test-cov                    # with coverage report

# Health check (against running server)
make health
```

## Architecture

### Overview

`javi-forecast` is the AIOps model server in the javi APM platform. It:

1. **Ingests** OTel telemetry from `javi-collector` via Kafka (spans, metrics, logs, deploy events)
2. **Maintains** an in-memory RED metrics feature store (Rate, Error rate, Latency percentiles)
3. **Forecasts** per-service RED metrics using time-series models
4. **Detects anomalies** and fires webhook/Slack alerts

### Startup sequence (`app/main.py` lifespan)

All shared objects are created in the `lifespan()` context manager and stored on `app.state.*`. API handlers access them via `request.app.state.*` — there is no DI framework.

Startup order:
1. Redis (optional, for multi-replica FeatureStore sharing)
2. ClickHouse connect + AlertStore preload
3. Redis preload → ClickHouse backfill of FeatureStore
4. Background engines start (order matters for dependencies):
   `Alerter → Kafka → BaselineComputer → AnomalyDetector → RCAEngine → JvmAnalyzer → BurnRateAnalyzer → VarForecaster → GrangerAnalyzer → Forecaster`
5. Service marked ready (`/readyz`)

### Data flow

```
javi-collector
    │
    ├─ Kafka: spans.all    → EventHandler       → FeatureStore (1-min RED buckets)
    ├─ Kafka: metrics      → MetricEventHandler → MetricFeatureStore
    ├─ Kafka: logs         → LogEventHandler    → LogStore (ChromaDB, if RAG enabled)
    └─ Kafka: deploys      → DeployEventHandler → DeploymentStore

FeatureStore (in-memory, Redis-backed)
    ├─ Forecaster          → ForecastStore (TTL cache) → GET /api/forecast/red
    ├─ VarForecaster       → cross-service VAR forecasting
    ├─ GrangerAnalyzer     → DependencyMap (Granger causality edges)
    └─ BurnRateAnalyzer    → SLO burn rate alerts

ClickHouse (apm DB)
    ├─ BaselineComputer    → hour-of-week RED baselines
    ├─ AnomalyDetector     → IsolationForest + z-score anomalies
    └─ RCAEngine           → causal hypothesis builder → IncidentStore (ChromaDB)
```

### Key modules

| Path | Role |
|---|---|
| `app/engine/feature_store.py` | Ring-buffer per-service RED metrics (deque, 72h @ 1-min cadence). Redis sync for HA. |
| `app/engine/forecaster.py` | Background loop: iterates services × RED dims, calls `selector.select_model()`, writes to `ForecastStore`. |
| `app/engine/selector.py` | Auto model selection: evaluates EWMA / ARIMA / Holt-Winters by cross-val MSE. |
| `app/engine/rca_engine.py` | Polls ClickHouse for anomalies → fetches correlated spans, topology neighbors, nearby deploys → builds hypothesis. |
| `app/consumer/kafka_consumer.py` | `aiokafka` consumer; routes by topic set to the correct handler; manual offset commit after each dispatch. |
| `app/store/clickhouse.py` | Async ClickHouse client (HTTP via `clickhouse-connect`). |
| `app/rag/` | Optional LLM features: Text-to-SQL (Anthropic Claude), IncidentStore, LogStore (both ChromaDB). |
| `app/anomaly/` | ML detectors: `EWMAForecaster`, `ARIMAForecaster`, `HoltWintersForecaster`, `IsolationForestDetector`, `STLAnomalyDetector`. |

### Configuration

All settings live in `app/config.py` (`pydantic-settings`, env file `.env`). Key toggles:

| Env var | Default | Purpose |
|---|---|---|
| `DISABLE_CLICKHOUSE` | `false` | Skip ClickHouse in local dev (`make dev` sets this) |
| `KAFKA_ENABLED` | `false` | Enable Kafka consumer |
| `DEFAULT_MODEL` | `ewma` | `ewma \| arima \| holtwinters \| auto` |
| `RAG_ENABLED` | `false` | Enable LLM/RAG features |
| `VAR_ENABLED` | `true` | Cross-service VAR forecaster |
| `GRANGER_ENABLED` | `true` | Granger causality analysis |
| `REDIS_URL` | `None` | FeatureStore Redis backend (needed for multi-replica) |

### Kafka message schema

Messages are JSON with a `schema_version: "1"` field (logged as warning if mismatch). Field names are **snake_case** throughout (collector→forecast contract). Span messages may be a single `SpanEvent`, a `SpanBatch` (`{"spans": [...]}`) or a bare JSON array.

### API surface

Docs at `http://localhost:8080/docs`. Key endpoints:

- `GET /healthz`, `GET /readyz` — liveness / readiness
- `POST /v1/spans` — direct span ingest (bypasses Kafka)
- `GET /api/forecast/red` — current RED metric forecasts
- `GET /dependency/graph`, `GET /dependency/{service}/causes` — Granger causality edges
- `GET /api/topology` — span-derived service topology
- `POST /api/rag/query` — Text-to-SQL query (requires `RAG_ENABLED=true`)
- `GET /metrics` — Prometheus metrics


<!-- AUTO-GENERATED:start (스크립트가 관리. 직접 수정 금지) -->

_아래 구간은 스크립트가 자동 생성합니다. 직접 수정하지 마세요._

### 기술 스택
- Python (`pyproject.toml`)
- Python (`requirements.txt`)
- Docker (`Dockerfile`)

### 명령어
**Make 타깃**:
```
clean
dev
docker-build
docker-down
docker-logs
docker-up
fmt
forecasts
health
install
k8s-apply
k8s-delete
k8s-logs
k8s-port-forward
k8s-rollout
lint
run
test
test-cov
test-ingest
```

### 최상위 디렉터리 구조
```
.github
app
scripts
tests
```

<!-- AUTO-GENERATED:end -->
