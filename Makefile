.DEFAULT_GOAL := help

GO ?= go
NPM ?= npm
CURL ?= curl
PYTHON ?= python3
BACKEND_URL ?= http://localhost:8080

.PHONY: help install update format lint test run clean threads-crawler-test \
	structure threads-crawler-status health_backend backend-run backend-test \
	backend-build backend-vet backend-health backend-status frontend-run \
	frontend-test frontend-lint frontend-build backend-check frontend-check check \
	collect-threads collect-pbplus

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
