.DEFAULT_GOAL := help

.PHONY: help install update format lint test run clean thread-crawler-test structure

help:
	@echo "Available commands:"
	@echo "  make install              - Install dependencies"
	@echo "  make update               - Update dependencies"
	@echo "  make format               - Format code"
	@echo "  make lint                 - Check code quality"
	@echo "  make test                 - Run tests"
	@echo "  make run                  - Run application"
	@echo "  make clean                - Remove generated files"
	@echo "  make thread-crawler-test  - Test Threads crawler"
	@echo "  make structure            - Show project structure"

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

thread-crawler-test:
	poetry run python \
		apps/collector/src/dsp_collector/sources/crawler/threads/crawler.py \
		--query "pickleball" \
		--limit 10 \
		--sort recent

structure:
	tree -I "node_modules"