from __future__ import annotations

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_asset_lifecycle_full_flow(sre_client: AsyncClient, ae_client: AsyncClient) -> None:
    # 1. SRE provisions a Database Endpoint
    ep_resp = await sre_client.post(
        "/v1/endpoints/database",
        json={
            "name": "db-prod",
            "credential_ref": "vault/secret/db",
            "technical_description": "Production Postgres",
        },
    )
    assert ep_resp.status_code == 201
    endpoint_id = ep_resp.json()["id"]

    # 2. AE registers a DataAsset (Draft)
    asset_resp = await ae_client.post(
        "/v1/assets/",
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
    asset_resp.json()["id"]
    assert asset_resp.json()["state"] == "draft"

    # 3. SRE activates the DataAsset by linking it to the Endpoint
    activate_resp = await sre_client.post(
        "/v1/assets/e2e_asset/activate", params={"endpoint_name": "db-prod"}
    )
    assert activate_resp.status_code == 200
    assert activate_resp.json()["state"] == "active"
    assert activate_resp.json()["endpoint_id"] == endpoint_id

    # 4. AE gets the Asset and verifies state
    get_resp = await ae_client.get("/v1/assets/e2e_asset")
    assert get_resp.status_code == 200
    assert get_resp.json()["state"] == "active"
    assert get_resp.json()["endpoint_id"] == endpoint_id
