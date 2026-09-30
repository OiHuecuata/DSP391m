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
	rm -rf .pytest_cache
	rm -rf .ruff_cache
	rm -rf dist
	rm -rf build
	find . -type d -name "__pycache__" -exec rm -rf {} +

thread-crawler-test:
	poetry run python \
		apps/collector/src/dsp_collector/sources/crawler/threads/crawler.py \
		--query "pickleball" \
		--limit 10 \
		--sort recent

structure:
	tree -I "node_modules"