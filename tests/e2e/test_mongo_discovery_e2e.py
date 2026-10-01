from __future__ import annotations

import asyncio
import os
import uuid

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
    suffix = uuid.uuid4().hex[:6]
    endpoint_name = f"e2e-mongo-{suffix}"
    asset_name = f"e2e-mongo-asset-{suffix}"

    # Register Endpoint — idempotent (409 accepted if already registered)
    await sre_client.post(
        "/v1/endpoints/nosql",
        json={
            "name": endpoint_name,
            "credential_ref": "secret/mongo",
            "technical_description": "MongoDB E2E test database",
        },
    )

    # Register Asset — idempotent (409 accepted)
    await api_client.post(
        "/v1/assets/",
        json={
            "name": asset_name,
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
        f"/v1/assets/{asset_name}/activate", params={"endpoint_name": endpoint_name}
    )

    # Trigger Discovery and assert that the run was accepted
    resp = await api_client.post(
        f"/v1/discovery/assets/{asset_name}/run", json={"triggered_by": "e2e_test"}
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
                text(
                    "SELECT name FROM data_objects WHERE name LIKE '%users_strict%' OR name LIKE '%logs_loose%'"
                )
            )
            names = [row[0] for row in result.fetchall()]
            if any("users_strict" in n for n in names) and any("logs_loose" in n for n in names):
                break
        await asyncio.sleep(2)

    assert any("users_strict" in n for n in names), f"users_strict not discovered. Found: {names}"
    assert any("logs_loose" in n for n in names), f"logs_loose not discovered. Found: {names}"

    await engine.dispose()
