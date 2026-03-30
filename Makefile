.PHONY: install dev lint test docker-build docker-up docker-down k8s-apply k8s-delete k8s-logs k8s-rollout clean

# ── Variables ─────────────────────────────────────────────────────────────────
IMAGE_NAME   ?= javi-forecast
IMAGE_TAG    ?= latest
K8S_NS       ?= apm
K8S_DIR      := k8s/forecast

# ── Local development ─────────────────────────────────────────────────────────

install:
	pip install -e ".[dev]"

## Run in dev mode (no ClickHouse, no Kafka)
dev:
	DISABLE_CLICKHOUSE=true \
	KAFKA_ENABLED=false \
	LOG_LEVEL=debug \
	uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload

## Run with full stack (requires docker-compose up first)
run:
	uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload

# ── Code quality ──────────────────────────────────────────────────────────────

lint:
	ruff check app/
	mypy app/ --ignore-missing-imports

fmt:
	ruff format app/

# ── Tests ─────────────────────────────────────────────────────────────────────

test:
	pytest tests/ -v

test-cov:
	pytest tests/ -v --cov=app --cov-report=term-missing

# ── Docker ────────────────────────────────────────────────────────────────────

docker-build:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

docker-up:
	docker compose up --build -d
	@echo "javi-forecast running at http://localhost:8080"
	@echo "Docs: http://localhost:8080/docs"

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f javi-forecast

# ── Kubernetes ────────────────────────────────────────────────────────────────

k8s-apply: docker-build
	@echo "Deploying to namespace: $(K8S_NS)"
	kubectl apply -f $(K8S_DIR)/

k8s-delete:
	kubectl delete -f $(K8S_DIR)/

k8s-logs:
	kubectl logs -n $(K8S_NS) -l app=javi-forecast -f

k8s-rollout: docker-build
	kubectl rollout restart -n $(K8S_NS) deployment/javi-forecast
	kubectl rollout status -n $(K8S_NS) deployment/javi-forecast

k8s-port-forward:
	kubectl port-forward -n $(K8S_NS) svc/javi-forecast 8080:8080

# ── Helpers ───────────────────────────────────────────────────────────────────

## Quick health check against running server
health:
	curl -s http://localhost:8080/healthz | python -m json.tool
	curl -s http://localhost:8080/readyz  | python -m json.tool

## Show current forecast results
forecasts:
	curl -s http://localhost:8080/api/forecast/red | python -m json.tool

## Ingest a test span batch
test-ingest:
	curl -s -X POST http://localhost:8080/v1/spans \
	  -H "Content-Type: application/json" \
	  -d '{"spans":[{"trace_id":"abc123","span_id":"s1","name":"GET /api","kind":1,"start_time_nano":1700000000000000000,"end_time_nano":1700000000150000000,"status_code":0,"service_name":"test-service"}]}' \
	  | python -m json.tool

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .mypy_cache .ruff_cache .pytest_cache dist build *.egg-info
