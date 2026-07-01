.PHONY: install sync dev test test-unit test-integration test-contract \
        coverage lint format format-check type-check check \
        migrate migrate-create migrate-downgrade docker-build docker-run

# --- Dependency management ---
install:
	uv sync --all-extras

sync:
	uv sync --all-extras --upgrade

# --- Development ---
dev:
	uv run uvicorn platform.main:app --reload --port 8000

# --- Testing ---
test:
	uv run pytest tests/ -v

test-unit:
	uv run pytest tests/unit/ -v

test-integration:
	uv run pytest tests/integration/ -v

test-contract:
	uv run pytest tests/contract/ -v

coverage:
	uv run pytest tests/ \
		--cov=platform \
		--cov-report=term-missing \
		--cov-report=html:htmlcov \
		--cov-fail-under=80

# --- Code quality ---
lint:
	uv run ruff check .

format:
	uv run ruff format .

format-check:
	uv run ruff format --check .

type-check:
	uv run mypy platform/

# Full CI gate: format + lint + type-check + all tests with coverage
check: format-check lint type-check coverage
	@echo "✅ All checks passed."

# --- Database ---
migrate:
	uv run alembic upgrade head

migrate-create:
	uv run alembic revision --autogenerate -m "$(name)"

migrate-downgrade:
	uv run alembic downgrade -1

# --- Docker ---
docker-build:
	docker build -t data-platform:dev .

docker-run:
	docker run --env-file .env -p 8000:8000 data-platform:dev
