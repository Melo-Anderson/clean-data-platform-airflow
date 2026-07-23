from __future__ import annotations

import asyncio
import os
import uuid

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = pytest.mark.e2e

_in_docker = os.path.exists("/.dockerenv") or os.getenv("API_URL", "").startswith(
    "http://platform-api"
)
_db_host = "postgres" if _in_docker else "localhost"
_mock_api_host = os.getenv("MOCK_API_HOST", "mock-api")

PLATFORM_DATABASE_URL = f"postgresql+asyncpg://airflow:airflow@{_db_host}:5432/platform_db"


@pytest.mark.asyncio
async def test_api_discovery_e2e(
    api_client: httpx.AsyncClient, sre_client: httpx.AsyncClient
) -> None:
    """E2E discovery test for REST API endpoints integrated with the platform API and metadata DB."""
    suffix = uuid.uuid4().hex[:6]
    endpoint_name = f"e2e-api-ep-{suffix}"
    asset_name = f"e2e-api-asset-{suffix}"

    # 1. Register Endpoint (SRE role)
    ep_resp = await sre_client.post(
        "/v1/endpoints/rest_api",
        json={
            "name": endpoint_name,
            "credential_ref": "secret/mock-store",
            "base_url": f"http://{_mock_api_host}:8081",
            "auth_type": "bearer",
            "technical_description": "Mock Store REST API E2E test endpoint",
        },
    )
    assert ep_resp.status_code == 201

    # 2. Register DataAsset
    asset_resp = await api_client.post(
        "/v1/assets/",
        json={
            "name": asset_name,
            "description": "REST API E2E data asset for OpenAPI and payload sampling discovery",
            "owner_email": "e2e@co.com",
            "tags": ["api", "e2e"],
            "policy_tags": [],
            "discovery_schedule": "0 0 * * *",
            "discovery_scope_include": ["*Product*", "*Customer*"],
            "discovery_scope_exclude": [],
        },
    )
    assert asset_resp.status_code == 201

    # 3. Activate DataAsset with Endpoint
    act_resp = await sre_client.post(
        f"/v1/assets/{asset_name}/activate", params={"endpoint_name": endpoint_name}
    )
    assert act_resp.status_code == 200

    # 4. Trigger Discovery
    resp = await api_client.post(
        f"/v1/discovery/assets/{asset_name}/run", json={"triggered_by": "e2e_test"}
    )
    assert resp.status_code == 201
    run_data = resp.json()
    print("DISCOVERY RESPONSE:", run_data)
    assert run_data.get("status") == "completed", (
        f"Discovery run failed: {run_data.get('error_message')}"
    )

    engine = create_async_engine(PLATFORM_DATABASE_URL)

    names: list[str] = []

    # 5. Poll platform metadata DB until DataObjects are persisted in data_objects table
    for _ in range(15):
        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT name FROM data_objects"))
            names = [row[0] for row in result.fetchall()]
            print("POLL NAMES:", names)
            if any("Product" in n for n in names) and any("Customer" in n for n in names):
                break
        await asyncio.sleep(2)

    assert any("Product" in n for n in names), (
        f"Product schema not discovered in data_objects table. Found: {names}"
    )
    assert any("Customer" in n for n in names), (
        f"Customer schema not discovered in data_objects table. Found: {names}"
    )

    await engine.dispose()
