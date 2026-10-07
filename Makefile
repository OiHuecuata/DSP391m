.DEFAULT_GOAL := help

GO ?= go
NPM ?= npm
CURL ?= curl
PYTHON ?= python3
BACKEND_URL ?= http://localhost:8080
KAFKA_SOCIAL_POST_TOPIC ?= $(shell PYTHONPATH=apps/collector/src poetry run python -c 'from dsp_collector.runtime.config import KAFKA_SOCIAL_POST_TOPIC; print(KAFKA_SOCIAL_POST_TOPIC)')

DSP_JAVA_HOME ?= $(if $(JAVA_HOME),$(JAVA_HOME),$(CURDIR)/.local/tools/java17)
SPARK_PYTHON = JAVA_HOME="$(DSP_JAVA_HOME)" PYTHONPATH=pipelines/spark/src:apps/collector/src poetry run python
SPARK_ARTIFACT ?=

.PHONY: help install update format lint test run clean threads-crawler-test \
	structure threads-crawler-status health_backend backend-run backend-test \
	backend-build backend-vet backend-health backend-status frontend-run \
	frontend-test frontend-lint frontend-build backend-check frontend-check check \
	collect-threads collect-pbplus collect-threads-kafka kafka-up kafka-down \
	kafka-status kafka-topic kafka-consume-social kafka-logs \
	spark-test spark-bronze-threads spark-bronze-pbplus spark-pbplus-silver \
	spark-social-stream spark-social-stream-once delta-inspect-social delta-inspect-courts spark-gold-social delta-inspect-gold-social databricks-build-wheel databricks-validate databricks-submit-social-gold

help:
	@echo "Available commands:"
	@echo "  make backend-run          - Run the backend development server"
	@echo "  make backend-test         - Run all backend Go tests"
	@echo "  make backend-build        - Build the backend server"
	@echo "  make backend-vet          - Run Go static analysis"
	@echo "  make backend-health       - Fetch backend health"
	@echo "  make backend-status       - Fetch and format system status JSON"
	@echo "  make frontend-run         - Run the frontend development server"
	@echo "  make frontend-test        - Build and run frontend browser tests"
	@echo "  make frontend-lint        - Run frontend lint"
	@echo "  make frontend-build       - Build the production frontend"
	@echo "  make backend-check        - Test, vet, and build the backend"
	@echo "  make frontend-check       - Lint, build, and test the frontend"
	@echo "  make check                - Validate both applications"
	@echo "  make health_backend       - Compatibility alias for backend-run"
	@echo ""
	@echo "Existing Python and project commands:"
	@echo "  make install              - Install dependencies"
	@echo "  make update               - Update dependencies"
	@echo "  make format               - Format code"
	@echo "  make lint                 - Check code quality"
	@echo "  make test                 - Run tests"
	@echo "  make run                  - Run application"
	@echo "  make clean                - Remove generated files"
	@echo "  make threads-crawler-test - Test Threads crawler"
	@echo "  make threads-crawler-status - Format the latest Threads snapshot"
	@echo "  make collect-threads      - Collect Threads into local Bronze/Silver JSON"
	@echo "  make collect-pbplus       - Collect PBPlus into local Bronze/Silver JSON"
	@echo "  make collect-threads-kafka - Collect Threads and publish saved Silver events"
	@echo "  make kafka-up / kafka-down - Start / stop local Kafka (retain data)"
	@echo "  make kafka-topic          - Create/describe the configured SocialPost topic"
	@echo "  make kafka-status / kafka-logs - Inspect local Kafka"
	@echo "  make kafka-consume-social - Read SocialPost events with keys"
	@echo "  make spark-test           - Run Spark/Delta tests without Kafka or AWS"
	@echo "  make spark-bronze-threads / spark-bronze-pbplus - Ingest SPARK_ARTIFACT"
	@echo "  make spark-pbplus-silver   - Ingest Court observations from SPARK_ARTIFACT"
	@echo "  make spark-social-stream / spark-social-stream-once - Stream Kafka to Delta"
	@echo "  make delta-inspect-social / delta-inspect-courts - Inspect Delta tables"
	@echo "  make spark-gold-court-supply / delta-inspect-gold-court-supply - Build/inspect venue supply"
	@echo "  make location-reference-validate - Check the versioned alias catalog"
	@echo "  make spark-gold-social     - Rebuild daily mentioned-location activity"
	@echo "  make spark-gold-market-signal / delta-inspect-gold-market-signal - Build/inspect rolling activity and current supply"
	@echo "  make databricks-validate-market-signal - Render analytical job offline"
	@echo "  make delta-inspect-gold-social - Inspect the Gold activity table"
	@echo "  make clickhouse-up / clickhouse-down / clickhouse-status - Local serving service"
	@echo "  make clickhouse-migrate - Apply typed serving DDL"
	@echo "  make load-market-signals-clickhouse - Replace serving state from Delta Gold"
	@echo "  make smoke-clickhouse - Compare Delta, repeated loads, SQL, and backend API"
	@echo "  make databricks-build-wheel - Package existing jobs and contracts"
	@echo "  make databricks-validate    - Validate/render configured job locally"
	@echo "  make databricks-submit-social-gold - Submit to explicitly configured compute"
	@echo "  make structure            - Show project structure"
	@echo ""
	@echo "HTTP helpers use BACKEND_URL=$(BACKEND_URL) (override with make BACKEND_URL=...)."

install:
	poetry install

update:
	poetry update

format:
	poetry run black .
	poetry run ruff check . --fix

lint:
	poetry run ruff check .
	poetry run black --check .

test:
	poetry run pytest

run:
	poetry run python main.py

clean:
	@echo "Cleaning project caches..."

	@echo "Cleaning Python caches..."
	find . -type d -name "__pycache__" -prune -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -prune -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -prune -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -prune -exec rm -rf {} +
	find . -type f \( -name "*.pyc" -o -name "*.pyo" \) -delete

	@echo "Cleaning Go caches..."
	go clean -cache
	go clean -testcache

	@echo "Cleaning Next.js caches..."
	find . -type d -name ".next" -prune -exec rm -rf {} +
	find . -type d -name ".turbo" -prune -exec rm -rf {} +

	@echo "Cleaning build artifacts..."
	find . -type d -name "dist" -prune -exec rm -rf {} +
	find . -type d -name "build" -prune -exec rm -rf {} +

	@echo "Clean complete."

threads-crawler-test:
	PYTHONPATH=apps/collector/src poetry run python \
		-m dsp_collector.sources.crawler.threads.crawler \
		--query "pickleball" \
		--limit 10 \
		--sort recent

collect-threads:
	PYTHONPATH=apps/collector/src poetry run python \
		-m dsp_collector.jobs.collect_social

collect-pbplus:
	PYTHONPATH=apps/collector/src poetry run python \
		-m dsp_collector.jobs.collect_courts

collect-threads-kafka:
	KAFKA_SOCIAL_POST_TOPIC="$(KAFKA_SOCIAL_POST_TOPIC)" PYTHONPATH=apps/collector/src poetry run python \
		-m dsp_collector.jobs.collect_social --publish-kafka

kafka-up:
	docker compose up -d --wait --wait-timeout 120 kafka

kafka-down:
	docker compose stop kafka

kafka-status:
	docker compose ps kafka

kafka-logs:
	docker compose logs --tail 100 kafka

kafka-topic:
	sh infra/kafka/create-topics.sh "$(KAFKA_SOCIAL_POST_TOPIC)"

kafka-consume-social:
	docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server kafka:29092 --topic "$(KAFKA_SOCIAL_POST_TOPIC)" \
		--from-beginning --property print.key=true --property key.separator=' | '

threads-crawler-status:
	@STATUS_FILE="apps/collector/.local/threads/status.json"; \
	if [ ! -f "$$STATUS_FILE" ]; then \
		echo "[threads-crawler] status file not found: $$STATUS_FILE"; \
		echo "[threads-crawler] run 'make threads-crawler-test' first"; \
		exit 1; \
	fi; \
	poetry run python -m json.tool "$$STATUS_FILE"

structure:
	tree -I "node_modules|__pycache__|*.pyc|.git|.next|test-results"

# Run Go commands inside the backend's own module. Each recipe retains its cwd.
backend-run:
	cd apps/backend && $(GO) run ./cmd/server

backend-test:
	cd apps/backend && $(GO) test ./...

backend-build:
	cd apps/backend && $(GO) build ./cmd/server

backend-vet:
	cd apps/backend && $(GO) vet ./...

backend-health:
	$(CURL) --fail --silent --show-error --max-time 10 "$(BACKEND_URL)/health"
	@printf '\n'

backend-status:
	@response=$$($(CURL) --fail --silent --show-error --max-time 10 "$(BACKEND_URL)/api/v1/system/status") && \
		printf '%s\n' "$$response" | $(PYTHON) -m json.tool

frontend-run:
	cd apps/frontend && $(NPM) run dev

frontend-lint:
	cd apps/frontend && $(NPM) run lint

frontend-build:
	cd apps/frontend && $(NPM) run build

# Playwright starts a temporary production server, so the build must finish first.
# Sharing this prerequisite also avoids duplicate builds in frontend-check/check.
frontend-test: frontend-build
	cd apps/frontend && $(NPM) run test:e2e

backend-check: backend-test backend-vet backend-build

frontend-check: frontend-lint frontend-build frontend-test

check: backend-check frontend-check

# This legacy name has always started the server rather than checked its health.
health_backend: backend-run

# Spark jobs are independent of collector commands. Artifacts are read-only inputs.
spark-test:
	$(SPARK_PYTHON) -m unittest discover -s pipelines/spark/test -p 'test_*.py'

spark-bronze-threads:
	@test -n "$(SPARK_ARTIFACT)" || (echo "Set SPARK_ARTIFACT to a Threads Bronze file"; exit 2)
	$(SPARK_PYTHON) -m dsp_spark.jobs.batch.ingest_bronze --source threads --artifact "$(SPARK_ARTIFACT)"

spark-bronze-pbplus:
	@test -n "$(SPARK_ARTIFACT)" || (echo "Set SPARK_ARTIFACT to a PBPlus Bronze file"; exit 2)
	$(SPARK_PYTHON) -m dsp_spark.jobs.batch.ingest_bronze --source pbplus --artifact "$(SPARK_ARTIFACT)"

spark-pbplus-silver:
	@test -n "$(SPARK_ARTIFACT)" || (echo "Set SPARK_ARTIFACT to a PBPlus Silver file"; exit 2)
	$(SPARK_PYTHON) -m dsp_spark.jobs.batch.ingest_pbplus --artifact "$(SPARK_ARTIFACT)"

spark-social-stream:
	$(SPARK_PYTHON) -m dsp_spark.jobs.streaming.social_kafka_to_delta

spark-social-stream-once:
	$(SPARK_PYTHON) -m dsp_spark.jobs.streaming.social_kafka_to_delta --available-now

delta-inspect-social:
	$(SPARK_PYTHON) -m dsp_spark.jobs.batch.inspect_delta --table social_posts

delta-inspect-courts:
	$(SPARK_PYTHON) -m dsp_spark.jobs.batch.inspect_delta --table courts

spark-gold-social:
	$(SPARK_PYTHON) -m dsp_spark.jobs.batch.build_social_activity_daily

delta-inspect-gold-social:
	$(SPARK_PYTHON) -m dsp_spark.jobs.batch.inspect_delta --table social_activity_daily

databricks-build-wheel:
	poetry run python infra/databricks/build_wheel.py

databricks-validate:
	sh scripts/submit_databricks_job.sh --dry-run

databricks-submit-social-gold:
	sh scripts/submit_databricks_job.sh

.PHONY: spark-gold-court-supply delta-inspect-gold-court-supply location-reference-validate databricks-validate-court-supply
spark-gold-court-supply:
	$(SPARK_PYTHON) -m dsp_spark.jobs.batch.build_court_supply_by_location

delta-inspect-gold-court-supply:
	$(SPARK_PYTHON) -m dsp_spark.jobs.batch.inspect_delta --table court_supply_by_location

location-reference-validate:
	$(SPARK_PYTHON) -m dsp_spark.processing.transform.location

databricks-validate-court-supply:
	sh scripts/submit_databricks_job.sh --job court_supply_by_location --dry-run

.PHONY: spark-gold-market-signal delta-inspect-gold-market-signal databricks-validate-market-signal
spark-gold-market-signal:
	$(SPARK_PYTHON) -m dsp_spark.jobs.batch.build_location_market_signal_snapshot

delta-inspect-gold-market-signal:
	$(SPARK_PYTHON) -m dsp_spark.jobs.batch.inspect_delta --table location_market_signal_snapshot

databricks-validate-market-signal:
	sh scripts/submit_databricks_job.sh --job location_market_signal_snapshot --dry-run

.PHONY: clickhouse-up clickhouse-down clickhouse-status clickhouse-migrate load-market-signals-clickhouse smoke-clickhouse
clickhouse-up:
	docker compose up -d --wait --wait-timeout 120 clickhouse

clickhouse-down:
	docker compose stop clickhouse

clickhouse-status:
	docker compose ps clickhouse

clickhouse-migrate:
	$(SPARK_PYTHON) -m dsp_spark.jobs.batch.load_market_signals_clickhouse --migrate-only

load-market-signals-clickhouse:
	$(SPARK_PYTHON) -m dsp_spark.jobs.batch.load_market_signals_clickhouse

smoke-clickhouse: clickhouse-up clickhouse-migrate
	$(SPARK_PYTHON) -m dsp_spark.jobs.batch.smoke_clickhouse
