FROM python:3.12-slim AS base

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

RUN useradd --create-home --shell /bin/bash appuser

WORKDIR /app

# Install dependencies in a separate layer for Docker cache efficiency
COPY pyproject.toml ./
RUN uv sync --no-dev --no-install-project

COPY platform/ ./platform/
COPY migrations/ ./migrations/
COPY alembic.ini ./

RUN uv sync --no-dev

USER appuser

EXPOSE 8000

# Runs DB migrations then starts the server
CMD ["sh", "-c", "uv run alembic upgrade head && uv run uvicorn platform.main:app --host 0.0.0.0 --port 8000"]
