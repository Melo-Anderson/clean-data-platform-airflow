# DuckDB E2E Test Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement an integration/E2E test for `DuckDbComputeAdapter` that uses real PostgreSQL and OpenBao to extract and save data to Parquet.

**Architecture:** Create `test_duckdb_adapter.py` inside `tests/integration/` to isolate adapter-specific integration testing from full platform E2E tests, verifying Vault credential resolution, DB connectivity, and Parquet/metrics output.

**Tech Stack:** pytest, DuckDB, PostgreSQL, OpenBao, Python.

## User Review Required
> [!IMPORTANT]
> The spec defines this as an E2E test, but it circumvents the API and Airflow by calling the adapter directly. This makes it an **Integration Test** according to the platform's `clean-code.md`.
> I've proposed placing it in `tests/integration/test_duckdb_adapter.py` instead of `tests/e2e/test_platform_e2e.py` to maintain architectural cleanliness.

## Global Constraints
- **Docker Environment:** The user will manually start and manage the Docker Compose environment. Tests will be executed via `pytest` directly on the host (not via `docker compose run`).
- **Pre-requisites:** Postgres and OpenBao must be accessible from the host environment.
- Verify Parquet output, `metrics.json` (`row_count` == 3) and `schema.json`.

---

### Task 1: Setup Test File and Database Fixtures

**Files:**
- Create: `tests/integration/test_duckdb_adapter.py`

**Interfaces:**
- Produces: `setup_postgres_table` fixture that populates `e2e_source_table` with 3 rows.

- [ ] **Step 1: Write test setup for PostgreSQL**

```python
import os
import pytest
import duckdb
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

# Defaults to localhost mappings since Docker is run manually by user
PLATFORM_DATABASE_URL = os.getenv("PLATFORM_DATABASE_URL", "postgresql+asyncpg://airflow:airflow@localhost:5432/platform_db")

@pytest.fixture(scope="module")
async def setup_postgres_table():
    engine = create_async_engine(PLATFORM_DATABASE_URL)
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS e2e_source_table;"))
        await conn.execute(text("""
            CREATE TABLE e2e_source_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))
        await conn.execute(text("INSERT INTO e2e_source_table (name) VALUES ('Test 1'), ('Test 2'), ('Test 3');"))
    await engine.dispose()
    yield
```

- [ ] **Step 2: Run pytest to ensure fixture syntax is valid**

Run: `pytest tests/integration/test_duckdb_adapter.py --setup-show`
Expected: PASS with fixture showing.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/test_duckdb_adapter.py
git commit -m "test(integration): setup postgres table fixture for duckdb tests"
```

### Task 2: Implement DuckDB E2E Extraction Test

**Files:**
- Modify: `tests/integration/test_duckdb_adapter.py`

**Interfaces:**
- Consumes: `setup_postgres_table`
- Produces: `test_duckdb_compute_adapter_e2e`

- [ ] **Step 1: Write the integration test**

```python
import json
import asyncio
from pathlib import Path
from app.infrastructure.compute.duckdb_adapter import DuckDbComputeAdapter

@pytest.mark.asyncio
@pytest.mark.e2e  # Marked as E2E so it can run in the compose test suite if needed
async def test_duckdb_compute_adapter_e2e(setup_postgres_table, tmp_path: Path):
    adapter = DuckDbComputeAdapter(output_dir=str(tmp_path))
    
    pipeline_id = "test_pipeline_e2e"
    # Config definition based on the adapter's expected structure
    config = {
        "credential_ref": "secret/postgres",
        "source_table": "public.e2e_source_table"
    }
    
    # Act
    run_id = await adapter.submit_job(pipeline_id, config)
    
    # Assert
    # Polling logic for async thread completion
    while True:
        status = await adapter.poll_job_status(run_id)
        if status in ["SUCCESS", "FAILED"]:
            break
        await asyncio.sleep(0.5)
        
    assert status == "SUCCESS"
    
    # Validate Parquet
    parquet_file = tmp_path / pipeline_id / run_id / "data.parquet"
    assert parquet_file.exists()
    
    # Validate Metrics
    metrics_file = tmp_path / pipeline_id / run_id / "metrics.json"
    assert metrics_file.exists()
    metrics = json.loads(metrics_file.read_text())
    assert metrics.get("row_count") == 3
    
    # Validate Schema
    schema_file = tmp_path / pipeline_id / run_id / "schema.json"
    assert schema_file.exists()
    schema = json.loads(schema_file.read_text())
    column_names = [col["name"] for col in schema]
    assert "id" in column_names
    assert "name" in column_names
    assert "created_at" in column_names
```

- [ ] **Step 2: Run test to verify it fails/passes**

Run: `pytest tests/integration/test_duckdb_adapter.py -v`
Expected: Passes if Adapter is fully implemented; otherwise fails pointing to what needs to be fixed.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/test_duckdb_adapter.py
git commit -m "test(integration): add duckdb adapter e2e extraction test"
```
