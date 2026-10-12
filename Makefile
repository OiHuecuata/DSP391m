.DEFAULT_GOAL := help

.PHONY: spark-intelligence intelligence-test intelligence-backfill load-intelligence-clickhouse
spark-intelligence:
	$(SPARK_PYTHON) -m dsp_spark.jobs.batch.build_intelligence

intelligence-test:
	$(SPARK_PYTHON) -m unittest discover -v -s pipelines/spark/test -p 'test_intelligence*.py'

intelligence-backfill:
	$(SPARK_PYTHON) -m dsp_spark.jobs.batch.ingest_bronze --backfill
	$(SPARK_PYTHON) -m dsp_spark.jobs.batch.build_intelligence

load-intelligence-clickhouse:
	$(SPARK_PYTHON) -m dsp_spark.jobs.batch.load_intelligence_clickhouse

GO ?= go
NPM ?= npm
CURL ?= curl
PYTHON ?= python3
BACKEND_URL ?= http://localhost:8080
KAFKA_SOCIAL_POST_TOPIC ?= $(shell PYTHONPATH=apps/collector/src poetry run python -c 'from dsp_collector.runtime.config import KAFKA_SOCIAL_POST_TOPIC; print(KAFKA_SOCIAL_POST_TOPIC)')

DSP_JAVA_HOME ?= $(shell PYTHONPATH=pipelines/spark/src $(PYTHON) -c 'from dsp_spark.runtime.config import default_java_home; print(default_java_home())')
SPARK_PYTHON = JAVA_HOME="$(DSP_JAVA_HOME)" PYTHONPATH=pipelines/spark/src:apps/collector/src poetry run python
SPARK_ARTIFACT ?=
AIRFLOW_COMPOSE = DSP_AIRFLOW_REPO_ROOT="$(CURDIR)" AIRFLOW_UID="$$(id -u)" docker compose --profile airflow
AIRFLOW_CONF ?= {}
AIRFLOW_RUN_ID ?=
# Pass JSON as an environment value, never as interpolated shell code.
export DSP_AIRFLOW_TRIGGER_CONF = $(AIRFLOW_CONF)
export DSP_AIRFLOW_TRIGGER_RUN_ID = $(AIRFLOW_RUN_ID)

.PHONY: help install update format lint test run clean threads-crawler-test \
	structure threads-crawler-status health_backend backend-run backend-test \
	backend-build backend-vet backend-health backend-status frontend-run \
	frontend-test frontend-lint frontend-build backend-check frontend-check check \
	collect-threads collect-pbplus collect-threads-kafka kafka-up kafka-down \
	kafka-status kafka-topic kafka-consume-social kafka-logs \
	spark-test spark-bronze-threads spark-bronze-pbplus spark-pbplus-silver \
	spark-social-stream spark-social-stream-once delta-inspect-social \
	delta-inspect-courts spark-gold-social delta-inspect-gold-social \
	databricks-build-wheel databricks-validate databricks-submit-social-gold \
	dev

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
	@echo "  make pipeline-e2e         - Verified finite local pipeline (PIPELINE_MODE=replay/live)"
	@echo "  make pipeline-preflight   - Validate E2E runtime, configuration and writable paths"
	@echo "  make pipeline-e2e-test    - Isolated replay-twice, empty-input and failure acceptance smoke"
	@echo "  make collect-live-eda     - Paced live accumulation with durable metrics/manifests"
	@echo "  make report-live-eda      - Measure local data quality/storage and export EDA Parquet"
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
	@echo "  make airflow-init / airflow-up - Initialize/start local batch orchestration"
	@echo "  make airflow-status / airflow-logs / airflow-dag-list - Inspect Airflow"
	@echo "  make airflow-trigger-market-signals - Trigger with AIRFLOW_CONF JSON"
	@echo "  make airflow-test / airflow-smoke / airflow-down - Validate/run smoke/stop"
	@echo "  make databricks-build-wheel - Package existing jobs and contracts"
	@echo "  make databricks-validate    - Validate/render configured job locally"
	@echo "  make databricks-submit-social-gold - Submit to explicitly configured compute"
	@echo "  make structure            - Show project structure"
	@echo ""
	@echo "HTTP helpers use BACKEND_URL=$(BACKEND_URL) (override with make BACKEND_URL=...)."

install:
	poetry install --no-root

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

.PHONY: airflow-init airflow-up airflow-down airflow-status airflow-logs airflow-dag-list airflow-trigger-market-signals airflow-test airflow-smoke
airflow-init:
	$(PYTHON) orchestration/airflow/init_local.py
	$(AIRFLOW_COMPOSE) build airflow-init
	$(AIRFLOW_COMPOSE) up -d --wait --wait-timeout 120 airflow-db
	$(AIRFLOW_COMPOSE) up -d --force-recreate --no-deps airflow-init
	$(AIRFLOW_COMPOSE) wait airflow-init

airflow-up: airflow-init
	$(AIRFLOW_COMPOSE) up -d --wait --wait-timeout 180 clickhouse airflow-api-server airflow-dag-processor airflow-scheduler

airflow-down:
	$(AIRFLOW_COMPOSE) stop airflow-scheduler airflow-dag-processor airflow-api-server airflow-db

airflow-status:
	$(AIRFLOW_COMPOSE) ps --all airflow-db airflow-init airflow-api-server airflow-scheduler airflow-dag-processor

airflow-logs:
	$(AIRFLOW_COMPOSE) logs --tail 100 airflow-api-server airflow-scheduler airflow-dag-processor

airflow-dag-list:
	$(AIRFLOW_COMPOSE) exec -T airflow-scheduler airflow dags list

airflow-trigger-market-signals:
	$(PYTHON) orchestration/airflow/control.py

airflow-test:
	$(AIRFLOW_COMPOSE) run --rm --no-deps airflow-init python -m unittest discover -s orchestration/airflow/test -p 'test_*.py'

airflow-smoke: airflow-up airflow-test
	$(PYTHON) orchestration/airflow/smoke.py

# Pass configuration as environment data, never interpolate user values into shell code.
PIPELINE_CONFIG_VARS = PIPELINE_MODE PIPELINE_ENV_FILE THREADS_REPLAY_PATH PBPLUS_REPLAY_PATH PIPELINE_STATE_DIR \
	PIPELINE_STREAM_TIMEOUT_SECONDS PIPELINE_COLLECTOR_TIMEOUT_SECONDS PIPELINE_THREADS_LIMIT PIPELINE_PBPLUS_LIMIT \
	KAFKA_BOOTSTRAP_SERVERS KAFKA_CLIENT_ID KAFKA_DELIVERY_TIMEOUT_MS KAFKA_REQUEST_TIMEOUT_MS KAFKA_RETRIES \
	DSP_KAFKA_BOOTSTRAP_SERVERS DSP_KAFKA_SOCIAL_POST_TOPIC DSP_KAFKA_STARTING_OFFSETS \
	DSP_SPARK_MASTER DSP_SPARK_APP_NAME DSP_SPARK_TRIGGER_INTERVAL DSP_SPARK_JARS DSP_SPARK_IVY_DIR \
	DSP_LAKEHOUSE_BASE_URI DSP_SPARK_SOCIAL_CHECKPOINT_PATH \
	DSP_DELTA_THREADS_BRONZE_PATH DSP_DELTA_PBPLUS_BRONZE_PATH \
	DSP_DELTA_SOCIAL_POSTS_PATH DSP_DELTA_COURTS_PATH DSP_DELTA_REJECTED_SOCIAL_PATH \
	DSP_DELTA_SOCIAL_ACTIVITY_DAILY_PATH DSP_DELTA_COURT_SUPPLY_BY_LOCATION_PATH \
	DSP_DELTA_UNRESOLVED_COURT_LOCATIONS_PATH DSP_DELTA_LOCATION_MARKET_SIGNAL_SNAPSHOT_PATH \
	DSP_MARKET_SIGNAL_WINDOW_DAYS DSP_MARKET_SIGNAL_END_DATE \
	CLICKHOUSE_HOST CLICKHOUSE_PORT CLICKHOUSE_DATABASE CLICKHOUSE_USER CLICKHOUSE_PASSWORD
# Export only defined variables: exporting an undefined Make variable injects an
# empty value, masking Python defaults and .env values.
$(foreach variable,$(PIPELINE_CONFIG_VARS),$(if $(filter undefined,$(origin $(variable))),,$(eval export $(variable))))
# Do not let this Makefile's computed topic default override a value from .env.
ifneq ($(filter command line environment environment override,$(origin KAFKA_SOCIAL_POST_TOPIC)),)
export KAFKA_SOCIAL_POST_TOPIC
endif

.PHONY: pipeline-e2e pipeline-preflight pipeline-e2e-test
pipeline-e2e:
	@$(SPARK_PYTHON) orchestration/local/pipeline.py

pipeline-preflight:
	@$(SPARK_PYTHON) orchestration/local/pipeline.py --preflight

pipeline-e2e-test:
	@$(SPARK_PYTHON) scripts/smoke_pipeline_e2e.py

LIVE_EDA_CONFIG ?= configs/collection/live_eda.v1.json
LIVE_EDA_OUTPUT ?= .local/live-eda
LIVE_EDA_CYCLES ?= 2
export LIVE_EDA_CONFIG LIVE_EDA_OUTPUT LIVE_EDA_CYCLES
.PHONY: collect-live-eda report-live-eda
collect-live-eda:
	@$(SPARK_PYTHON) scripts/collect_live_dataset.py

report-live-eda:
	@$(SPARK_PYTHON) scripts/report_live_dataset.py --export

.PHONY: spark-bronze-backfill spark-bronze-youtube
spark-bronze-backfill:
	$(SPARK_PYTHON) -m dsp_spark.jobs.batch.ingest_bronze --backfill

spark-bronze-youtube:
	$(SPARK_PYTHON) -m dsp_spark.jobs.batch.ingest_bronze --source youtube --artifact "$(SPARK_ARTIFACT)"

.PHONY: spark-bronze-social-silver spark-bronze-retention-plan
spark-bronze-social-silver:
	$(SPARK_PYTHON) -m dsp_spark.jobs.batch.ingest_social_bronze

spark-bronze-retention-plan:
	$(SPARK_PYTHON) -m dsp_spark.jobs.batch.bronze_retention

.PHONY: spark-bronze-status
spark-bronze-status:
	$(SPARK_PYTHON) -m dsp_spark.jobs.batch.ingest_bronze --status-file "$(SPARK_STATUS_FILE)"

dev:
	$(MAKE) kafka-up clickhouse-up
	JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 $(MAKE) -j2 backend-run frontend-run
