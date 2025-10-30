# ==== Config ====
VENV ?= .venv
VENV_BIN := $(VENV)/bin
PYTHON := $(VENV_BIN)/python
PIP := $(PYTHON) -m pip
PYTEST := $(PYTHON) -m pytest
COVERAGE := $(PYTHON) -m coverage
LOADER_CMD ?= $(PYTHON) -m data_loader.cli

REQUIREMENTS ?= requirements.txt
REQUIREMENTS_DEV ?= requirements-dev.txt

# Test config and DB
TEST_CONFIG ?= tests/test_config.toml
TEST_DB_FILE ?= tests/data_test.db

.DEFAULT_GOAL := help

.PHONY: help venv install dev test itest coverage clean freeze lint format check test-prepare test-clean

# ==== Help ====
help: ## Show this help
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(lastword $(MAKEFILE_LIST)) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

# ==== Environment ====
venv: ## Create virtual environment (once)
	@python3 -m venv $(VENV)
	@$(VENV_BIN)/python -m pip install --upgrade pip setuptools wheel

upgrade: venv ## Upgrade build tooling inside venv
	@$(PIP) install --upgrade pip setuptools wheel

# ==== Install ====
install: venv ## Install module
	@$(PIP) install .

dev: venv ## Editable install of the project for development
	@$(PIP) install -e ".[dev]"

freeze: venv ## Export exact environment (lockfile)
	@$(PIP) freeze > requirements.lock.txt
	@echo "Wrote requirements.lock.txt"

# ==== Quality & Tests ====
lint: dev ## Run linters if available (ruff/mypy)
	-@$(VENV_BIN)/ruff check .
	-@$(VENV_BIN)/mypy .

format: dev ## Apply formatting if available (ruff/black)
	-@$(VENV_BIN)/ruff format .
	-@$(VENV_BIN)/black .

check: lint test ## Lint + test combo

test-prepare: ## Ensure test directories exist
	@mkdir -p tests/data

test: dev test-prepare ## Run integration load (SQLite) and then unit tests
	@echo "Running loader with $(TEST_CONFIG) ..."
	$(PYTHON) -m data_loader.cli --config $(TEST_CONFIG)
	@echo "Running pytest ..."
	@$(PYTEST) -q tests

itest: dev test-prepare ## Only integration load (no unit tests)
	$(PYTHON) -m data_loader.cli --config $(TEST_CONFIG)

coverage: dev ## Run tests with coverage report
	@$(COVERAGE) run -m pytest -q
	@$(COVERAGE) report -m
	@echo "HTML report: htmlcov/index.html"
	-@$(COVERAGE) html

# ==== Clean ====
clean: ## Remove venv and Python/build caches
	@rm -rf $(VENV) .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov build dist *.egg-info
	@find . -type d -name "__pycache__" -exec rm -rf {} +
	@echo "To remove SQLite test DB, run: make test-clean"

test-clean: ## Remove SQLite test DB file
	@rm -f $(TEST_DB_FILE)
