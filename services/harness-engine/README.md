# YAML Harness Engine AI

AI-assisted YAML pipeline specification generator. Uses LangGraph with deterministic guardrails.

## Setup
```bash
cd services/harness-engine && cp .env.example .env && uv sync
```

## CLI
```bash
uv run python -m harness_engine.cli generate "Ingest sales table daily at 6am"
uv run python -m harness_engine.cli generate "ETL with dbt" --save-to pipeline.yaml
```

## API
```bash
uv run uvicorn src.infrastructure.api.app:app --reload
curl -X POST http://localhost:8000/api/v1/generate-yaml -H "Content-Type: application/json" -d '{"prompt": "Ingest Oracle sales table"}'
```

## Tests
```bash
uv run pytest tests/ -v
```

## Environment Variables
| Variable | Required | Default | Description |
|---|---|---|---|
| `OPENAI_API_KEY` | Yes | — | OpenAI API key |
| `OPENAI_MODEL` | No | `gpt-4o` | Model name |
| `PLATFORM_DB_URL` | Yes | — | Read-only SQLAlchemy DSN |
| `METRICS_STORAGE_PATH` | No | `./data/metrics` | Path to metrics.json files |
| `MAX_ITERATIONS` | No | `3` | Max guardrail retry loops |
| `LANGSMITH_API_KEY` | No | — | LangSmith tracing |
