# Job Scheduler Microservice Makefile
# -----------------------------------

.PHONY: help setup clean install update lint format check mypy test test-unit test-integration test-cov test-report
.PHONY: migrate migrate-generate migrate-upgrade migrate-downgrade db-init db-reset
.PHONY: run run-api run-worker run-beat run-all
.PHONY: docker-build docker-run docker-stop docker-clean docker-prune
.PHONY: deploy deploy-staging deploy-prod logs backup

# Default target when just running `make`
.DEFAULT_GOAL := help

# Environment variables
PYTHON := python
PIP := pip
POETRY := poetry
PYTEST := pytest
DOCKER := docker
DOCKER_COMPOSE := docker-compose
ALEMBIC := alembic
UVICORN := uvicorn
CELERY := celery

# Project settings
PROJECT_NAME := job-scheduler-microservice
PYTHON_VERSION := 3.10
MODULE_NAME := scheduler
API_PORT := 8000
API_HOST := 0.0.0.0
API_WORKERS := 4
DOCKER_COMPOSE_FILE := docker/docker-compose.yml
DOCKER_IMAGE_NAME := scheduler-microservice

# Colors for terminal output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

# Help target - lists all available targets with descriptions
help: ## Show this help message
	@echo "${BLUE}Job Scheduler Microservice${NC} - Makefile Targets"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  ${YELLOW}%-20s${NC} %s\n", $$1, $$2}'

# =========================
# Development Environment
# =========================

setup: ## Set up development environment
	@echo "${BLUE}Setting up development environment...${NC}"
	$(PYTHON) -m venv venv
	@echo "${GREEN}Created virtual environment in ./venv/${NC}"
	@echo "Activate with: ${YELLOW}source venv/bin/activate${NC} (Unix) or ${YELLOW}venv\\Scripts\\activate${NC} (Windows)"
	@echo "Then run: ${YELLOW}make install${NC}"

install: ## Install dependencies
	@echo "${BLUE}Installing dependencies...${NC}"
	$(PIP) install --upgrade pip
	$(PIP) install poetry
	$(POETRY) install --no-root
	$(POETRY) run pre-commit install
	@echo "${GREEN}Dependencies installed successfully${NC}"

update: ## Update dependencies
	@echo "${BLUE}Updating dependencies...${NC}"
	$(POETRY) update
	@echo "${GREEN}Dependencies updated successfully${NC}"

clean: ## Clean up environment and build artifacts
	@echo "${BLUE}Cleaning project...${NC}"
	rm -rf build/ dist/ *.egg-info/ .coverage htmlcov/ .pytest_cache/ .mypy_cache/ __pycache__/ */__pycache__/ */*/__pycache__/
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	@echo "${GREEN}Cleaned up project artifacts${NC}"

# =========================
# Code Quality
# =========================

lint: ## Run linters (flake8, etc.)
	@echo "${BLUE}Running linters...${NC}"
	$(POETRY) run flake8 $(MODULE_NAME) tests
	@echo "${GREEN}Linting completed${NC}"

format: ## Format code with black and isort
	@echo "${BLUE}Formatting code...${NC}"
	$(POETRY) run black $(MODULE_NAME) tests
	$(POETRY) run isort $(MODULE_NAME) tests
	@echo "${GREEN}Formatting completed${NC}"

check: ## Check code formatting without making changes
	@echo "${BLUE}Checking code formatting...${NC}"
	$(POETRY) run black --check $(MODULE_NAME) tests
	$(POETRY) run isort --check $(MODULE_NAME) tests
	@echo "${GREEN}Format check completed${NC}"

mypy: ## Run type checking
	@echo "${BLUE}Running type checking...${NC}"
	$(POETRY) run mypy $(MODULE_NAME)
	@echo "${GREEN}Type checking completed${NC}"

security: ## Run security checks
	@echo "${BLUE}Running security checks...${NC}"
	$(POETRY) run safety check
	$(POETRY) run bandit -r $(MODULE_NAME)
	@echo "${GREEN}Security checks completed${NC}"

validate: lint check mypy security ## Run all code quality checks

# =========================
# Testing
# =========================

test: ## Run all tests
	@echo "${BLUE}Running all tests...${NC}"
	$(POETRY) run $(PYTEST)
	@echo "${GREEN}All tests passed${NC}"

test-unit: ## Run unit tests only
	@echo "${BLUE}Running unit tests...${NC}"
	$(POETRY) run $(PYTEST) -m "unit" -v
	@echo "${GREEN}Unit tests passed${NC}"

test-integration: ## Run integration tests only
	@echo "${BLUE}Running integration tests...${NC}"
	$(POETRY) run $(PYTEST) -m "integration" -v
	@echo "${GREEN}Integration tests passed${NC}"

test-cov: ## Run tests with coverage
	@echo "${BLUE}Running tests with coverage...${NC}"
	$(POETRY) run $(PYTEST) --cov=$(MODULE_NAME) --cov-report=term-missing
	@echo "${GREEN}Coverage tests completed${NC}"

test-report: ## Generate HTML coverage report
	@echo "${BLUE}Generating coverage report...${NC}"
	$(POETRY) run $(PYTEST) --cov=$(MODULE_NAME) --cov-report=html
	@echo "${GREEN}Coverage report generated in htmlcov/${NC}"

# =========================
# Database Operations
# =========================

migrate-generate: ## Generate a new migration (usage: make migrate-generate message="description")
	@echo "${BLUE}Generating new migration...${NC}"
	$(POETRY) run $(ALEMBIC) revision --autogenerate -m "$(message)"
	@echo "${GREEN}Migration generated${NC}"

migrate-upgrade: ## Upgrade database to the latest version
	@echo "${BLUE}Upgrading database...${NC}"
	$(POETRY) run $(ALEMBIC) upgrade head
	@echo "${GREEN}Database upgraded${NC}"

migrate-downgrade: ## Downgrade database by one version
	@echo "${BLUE}Downgrading database...${NC}"
	$(POETRY) run $(ALEMBIC) downgrade -1
	@echo "${GREEN}Database downgraded${NC}"

migrate: migrate-upgrade ## Alias for migrate-upgrade

db-init: ## Initialize database
	@echo "${BLUE}Initializing database...${NC}"
	$(POETRY) run $(PYTHON) -m $(MODULE_NAME).scripts.init_db
	@echo "${GREEN}Database initialized${NC}"

db-reset: ## Reset database (WARNING: DESTRUCTIVE)
	@echo "${RED}WARNING: This will delete all data!${NC}"
	@read -p "Are you sure you want to reset the database? [y/N] " confirm && \
		if [ "$$confirm" = "y" ] || [ "$$confirm" = "Y" ]; then \
			echo "${BLUE}Resetting database...${NC}"; \
			$(POETRY) run $(PYTHON) -m $(MODULE_NAME).scripts.reset_db; \
			echo "${GREEN}Database reset${NC}"; \
		else \
			echo "${YELLOW}Database reset canceled${NC}"; \
		fi

# =========================
# Run Application
# =========================

run-api: ## Run API server
	@echo "${BLUE}Starting API server...${NC}"
	$(POETRY) run $(UVICORN) $(MODULE_NAME).main:app --host $(API_HOST) --port $(API_PORT) --reload
	@echo "${GREEN}API server stopped${NC}"

run-worker: ## Run Celery worker
	@echo "${BLUE}Starting Celery worker...${NC}"
	$(POETRY) run $(CELERY) -A $(MODULE_NAME).tasks.celery_app worker --loglevel=info
	@echo "${GREEN}Celery worker stopped${NC}"

run-beat: ## Run Celery beat
	@echo "${BLUE}Starting Celery beat...${NC}"
	$(POETRY) run $(CELERY) -A $(MODULE_NAME).tasks.celery_app beat --loglevel=info
	@echo "${GREEN}Celery beat stopped${NC}"

run-flower: ## Run Celery flower monitoring
	@echo "${BLUE}Starting Celery flower...${NC}"
	$(POETRY) run $(CELERY) -A $(MODULE_NAME).tasks.celery_app flower --port=5555
	@echo "${GREEN}Celery flower stopped${NC}"

run: run-api ## Alias for run-api

# =========================
# Docker Operations
# =========================

docker-build: ## Build Docker images
	@echo "${BLUE}Building Docker images...${NC}"
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) build
	@echo "${GREEN}Docker images built${NC}"

docker-run: ## Run all services in Docker
	@echo "${BLUE}Starting all services in Docker...${NC}"
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) up -d
	@echo "${GREEN}Services started. Access API at http://localhost:$(API_PORT)${NC}"
	@echo "Access Flower at http://localhost:5555"
	@echo "Access Grafana at http://localhost:3000"

docker-stop: ## Stop all Docker services
	@echo "${BLUE}Stopping all services...${NC}"
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) down
	@echo "${GREEN}Services stopped${NC}"

docker-logs: ## View Docker logs
	@echo "${BLUE}Showing service logs...${NC}"
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) logs -f

docker-clean: ## Remove all Docker containers, networks and volumes
	@echo "${BLUE}Cleaning up Docker resources...${NC}"
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) down -v --remove-orphans
	@echo "${GREEN}Docker resources cleaned${NC}"

docker-prune: ## Prune all unused Docker resources
	@echo "${RED}WARNING: This will remove all unused Docker resources!${NC}"
	@read -p "Are you sure you want to continue? [y/N] " confirm && \
		if [ "$$confirm" = "y" ] || [ "$$confirm" = "Y" ]; then \
			echo "${BLUE}Pruning Docker resources...${NC}"; \
			$(DOCKER) system prune -af; \
			$(DOCKER) volume prune -f; \
			echo "${GREEN}Docker resources pruned${NC}"; \
		else \
			echo "${YELLOW}Docker pruning canceled${NC}"; \
		fi

# =========================
# Deployment
# =========================

deploy-staging: ## Deploy to staging environment
	@echo "${BLUE}Deploying to staging...${NC}"
	# Add deployment commands here
	@echo "${GREEN}Deployed to staging${NC}"

deploy-prod: ## Deploy to production environment
	@echo "${RED}WARNING: Deploying to PRODUCTION!${NC}"
	@read -p "Are you sure you want to deploy to production? [y/N] " confirm && \
		if [ "$$confirm" = "y" ] || [ "$$confirm" = "Y" ]; then \
			echo "${BLUE}Deploying to production...${NC}"; \
			# Add production deployment commands here; \
			echo "${GREEN}Deployed to production${NC}"; \
		else \
			echo "${YELLOW}Production deployment canceled${NC}"; \
		fi

deploy: deploy-staging ## Alias for deploy-staging

# =========================
# Utility Commands
# =========================

logs: ## View application logs
	@echo "${BLUE}Showing application logs...${NC}"
	tail -f logs/scheduler.log

backup: ## Backup database
	@echo "${BLUE}Backing up database...${NC}"
	$(POETRY) run $(PYTHON) -m $(MODULE_NAME).scripts.backup_db
	@echo "${GREEN}Database backup created${NC}"

version: ## Show application version
	@echo "${BLUE}Application version:${NC}"
	@grep -m 1 "version" pyproject.toml | cut -d '"' -f 2