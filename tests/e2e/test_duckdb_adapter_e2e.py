import os
import pytest
import duckdb
import json
import asyncio
from pathlib import Path
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from app.infrastructure.adapters.compute.duckdb_compute_adapter import DuckDbComputeAdapter

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

from app.infrastructure.adapters.secrets.bao_secret_manager_adapter import BaoSecretManagerAdapter

from app.infrastructure.airflow_callbacks.compute_job_adapter import JobStatus

@pytest.mark.asyncio
@pytest.mark.e2e
async def test_duckdb_compute_adapter_e2e(setup_postgres_table, tmp_path: Path):
    secret_manager = BaoSecretManagerAdapter(
        vault_url=os.getenv("PLATFORM_VAULT_URL", os.getenv("VAULT_URL", "http://localhost:8200")),
        vault_token=os.getenv("PLATFORM_VAULT_TOKEN", os.getenv("VAULT_TOKEN", "root"))
    )
    adapter = DuckDbComputeAdapter(secret_manager=secret_manager, output_base_dir=str(tmp_path))
    
    pipeline_id = "test_pipeline_e2e"
    config = {
        "credential_ref": "secret/postgres",
        "source_table": "public.e2e_source_table"
    }
    
    run_id = adapter.submit_job(pipeline_id, "ingestion", config)
    
    while True:
        result = adapter.poll_job_status(run_id)
        if result.status in [JobStatus.SUCCESS, JobStatus.FAILED]:
            break
        await asyncio.sleep(0.5)
        
    assert result.status == JobStatus.SUCCESS
    
    parquet_file = tmp_path / pipeline_id / run_id / "data.parquet"
    assert parquet_file.exists()
    
    metrics_file = tmp_path / pipeline_id / run_id / "metrics.json"
    assert metrics_file.exists()
    metrics = json.loads(metrics_file.read_text())
    assert metrics.get("row_count") == 3
    
    schema_file = tmp_path / pipeline_id / run_id / "schema.json"
    assert schema_file.exists()
    schema = json.loads(schema_file.read_text())
    column_names = [col["name"] for col in schema]
    assert "id" in column_names
    assert "name" in column_names
    assert "created_at" in column_names
