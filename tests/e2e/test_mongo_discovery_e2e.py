from __future__ import annotations

import asyncio
import os

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.e2e

_in_docker = os.path.exists("/.dockerenv") or os.getenv("API_URL", "").startswith(
    "http://platform-api"
)
_db_host = "postgres" if _in_docker else "localhost"

PLATFORM_DATABASE_URL = os.getenv(
    "PLATFORM_DATABASE_URL", f"postgresql+asyncpg://airflow:airflow@{_db_host}:5432/platform_db"
)


@pytest.mark.asyncio
async def test_mongo_discovery_e2e(
    api_client: httpx.AsyncClient, sre_client: httpx.AsyncClient
) -> None:
    # Register Endpoint — idempotent (409 accepted if already registered)
    await sre_client.post(
        "/v1/endpoints/nosql",
        json={
            "name": "e2e-mongo",
            "credential_ref": "secret/mongo",
            "technical_description": "MongoDB E2E test database",
        },
    )

    # Register Asset — idempotent (409 accepted)
    await api_client.post(
        "/v1/assets/",
        json={
            "name": "e2e-mongo-asset",
            "description": "MongoDB E2E data asset for hybrid discovery testing",
            "owner_email": "e2e@co.com",
            "tags": ["mongo", "e2e"],
            "policy_tags": [],
            "discovery_schedule": "0 0 * * *",
            "discovery_scope_include": ["test_db.*"],
            "discovery_scope_exclude": [],
        },
    )

    # Activate (SRE role required — see business_rules.md Fluxo A)
    await sre_client.post(
        "/v1/assets/e2e-mongo-asset/activate", params={"endpoint_name": "e2e-mongo"}
    )

    # Trigger Discovery and assert that the run was accepted
    resp = await api_client.post(
        "/v1/discovery/assets/e2e-mongo-asset/run", json={"triggered_by": "e2e_test"}
    )
    assert resp.status_code == 201

    engine = create_async_engine(PLATFORM_DATABASE_URL)
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    # Initialise before loop to guarantee variable is always bound
    names: list[str] = []

    # Poll platform metadata DB until both DataObjects are persisted (max 20s)
    for _ in range(10):
        async with async_session() as session:
            result = await session.execute(
                text("SELECT name FROM data_objects WHERE name LIKE 'test_db.%'")
            )
            names = [row[0] for row in result.fetchall()]
            if "test_db.users_strict" in names and "test_db.logs_loose" in names:
                break
        await asyncio.sleep(2)

    assert "test_db.users_strict" in names, f"users_strict not discovered. Found: {names}"
    assert "test_db.logs_loose" in names, f"logs_loose not discovered. Found: {names}"

    await engine.dispose()
