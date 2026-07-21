from __future__ import annotations

import asyncio
import os
import time

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.e2e

_in_docker = os.path.exists("/.dockerenv") or os.getenv("API_URL", "").startswith(
    "http://platform-api"
)
_db_host = "postgres" if _in_docker else "localhost"
_perf_db_host = "postgres-perf" if _in_docker else "localhost"
_perf_db_port = "5432" if _in_docker else "5433"

PLATFORM_DATABASE_URL = os.getenv(
    "PLATFORM_DATABASE_URL", f"postgresql+asyncpg://airflow:airflow@{_db_host}:5432/platform_db"
)
# Connection URL targeting the isolated perf database
PERF_DATABASE_URL = os.getenv(
    "PERF_DATABASE_URL",
    f"postgresql+asyncpg://airflow:airflow@{_perf_db_host}:{_perf_db_port}/perf_db",
)


@pytest.mark.asyncio
async def test_postgres_perf_e2e(
    api_client: httpx.AsyncClient, sre_client: httpx.AsyncClient
) -> None:
    # Register Endpoint — points to the postgres-perf container (credential seeded in openbao-init)
    await sre_client.post(
        "/v1/endpoints/database",
        json={
            "name": "e2e-pg-perf",
            "credential_ref": "secret/pg_perf",
            "technical_description": "Isolated Postgres container with 300+ synthetic tables for structural performance testing",
        },
    )

    # Register Asset
    await api_client.post(
        "/v1/assets/",
        json={
            "name": "e2e-pg-perf-asset",
            "description": "Postgres structural performance asset",
            "owner_email": "e2e@co.com",
            "tags": ["perf", "e2e"],
            "policy_tags": [],
            "discovery_schedule": "0 0 * * *",
            "discovery_scope_include": ["public.*"],
            "discovery_scope_exclude": [],
        },
    )

    # Activate (SRE role required)
    await sre_client.post(
        "/v1/assets/e2e-pg-perf-asset/activate", params={"endpoint_name": "e2e-pg-perf"}
    )

    # Use monotonic clock to avoid wall-clock drift in CI environments
    start_time = time.monotonic()
    resp = await api_client.post(
        "/v1/discovery/assets/e2e-pg-perf-asset/run", json={"triggered_by": "perf_test"}
    )
    assert resp.status_code == 201

    engine = create_async_engine(PLATFORM_DATABASE_URL)
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    found_count = 0
    # Poll platform metadata DB until all 300 synthetic DataObjects are persisted (max 30s)
    for _ in range(15):
        async with async_session() as session:
            result = await session.execute(
                text("SELECT count(*) FROM data_objects WHERE name LIKE 'public.synthetic_table_%'")
            )
            found_count = result.scalar() or 0
            if found_count >= 300:
                break
        await asyncio.sleep(2)

    elapsed = time.monotonic() - start_time

    assert found_count >= 300, f"Expected 300+ synthetic tables in metadata DB, found {found_count}"

    # Validate that the edge case table with exotic types was also discovered
    async with async_session() as session:
        result = await session.execute(
            text("SELECT name FROM data_objects WHERE name = 'public.edge_case_table'")
        )
        assert result.fetchone() is not None, "edge_case_table missing from Discovery results"

    # SLA: full structural discovery of 300 tables must complete under 30 seconds locally
    assert elapsed < 30.0, f"Discovery SLA breached: took {elapsed:.2f}s (limit: 30s)"

    await engine.dispose()
