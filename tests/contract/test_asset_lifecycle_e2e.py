from __future__ import annotations

import pytest
from httpx import AsyncClient

from app.auth.dependencies import get_current_user
from app.auth.role import Role
from tests.conftest import _override


@pytest.mark.asyncio
async def test_asset_lifecycle_full_flow(client: AsyncClient, app) -> None:
    # 1. SRE provisions a Database Endpoint
    app.dependency_overrides[get_current_user] = _override(Role.SRE)
    ep_resp = await client.post(
        "/endpoints/database",
        json={
            "asset_id": "temp-asset-id",  # Will be ignored mostly by the repo initially
            "credential_ref": "/vault/creds/db",
        },
    )
    assert ep_resp.status_code == 201
    endpoint_id = ep_resp.json()["id"]

    # 2. AE registers a DataAsset (Draft)
    app.dependency_overrides[get_current_user] = _override(Role.ANALYTICS_ENGINEER)
    asset_resp = await client.post(
        "/assets/",
        json={
            "name": "e2e_asset",
            "description": "Full flow test",
            "owner_email": "e2e@co.com",
            "tags": ["test"],
            "policy_tags": [],
            "discovery_schedule": "0 0 * * *",
            "discovery_scope_include": ["*"],
            "discovery_scope_exclude": [],
        },
    )
    assert asset_resp.status_code == 201
    asset_id = asset_resp.json()["id"]
    assert asset_resp.json()["state"] == "draft"

    # 3. SRE activates the DataAsset by linking it to the Endpoint
    app.dependency_overrides[get_current_user] = _override(Role.SRE)
    activate_resp = await client.post(
        f"/assets/{asset_id}/activate", params={"endpoint_id": endpoint_id}
    )
    assert activate_resp.status_code == 200
    assert activate_resp.json()["state"] == "active"
    assert activate_resp.json()["endpoint_id"] == endpoint_id

    # 4. AE gets the Asset and verifies state
    app.dependency_overrides[get_current_user] = _override(Role.ANALYTICS_ENGINEER)
    get_resp = await client.get(f"/assets/{asset_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["state"] == "active"
    assert get_resp.json()["endpoint_id"] == endpoint_id
