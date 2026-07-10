from __future__ import annotations

import pytest
from httpx import AsyncClient

from app.auth.current_user import CurrentUser
from app.auth.role import Role
from app.domain.shared.value_objects import EmailAddress


def _override(role: Role):
    user = CurrentUser(id="u1", email=EmailAddress("test@co.com"), role=role)
    return lambda: user


@pytest.mark.asyncio
async def test_create_asset_returns_201_in_draft(ae_client: AsyncClient) -> None:
    response = await ae_client.post(
        "/v1/assets/",
        json={
            "name": "contract_asset",
            "description": "Test",
            "owner_email": "ae@co.com",
            "tags": ["core"],
            "policy_tags": ["PII"],
            "discovery_schedule": "0 6 * * *",
            "discovery_scope_include": ["customers"],
            "discovery_scope_exclude": [],
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["state"] == "draft"
    assert body["policy_tags"] == ["PII"]
    assert "id" in body


@pytest.mark.asyncio
async def test_invalid_cron_returns_422(ae_client: AsyncClient) -> None:
    response = await ae_client.post(
        "/v1/assets/",
        json={
            "name": "bad_sched",
            "description": "Test",
            "owner_email": "ae@co.com",
            "tags": [],
            "policy_tags": [],
            "discovery_schedule": "not-a-cron",
            "discovery_scope_include": [],
            "discovery_scope_exclude": [],
        },
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_sre_cannot_create_asset(sre_client: AsyncClient) -> None:
    response = await sre_client.post(
        "/v1/assets/",
        json={
            "name": "sre_asset",
            "description": "Test",
            "owner_email": "sre@co.com",
            "tags": [],
            "policy_tags": [],
            "discovery_schedule": "0 6 * * *",
            "discovery_scope_include": [],
            "discovery_scope_exclude": [],
        },
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_get_nonexistent_asset_returns_404(ae_client: AsyncClient) -> None:
    response = await ae_client.get("/v1/assets/00000000-0000-0000-0000-000000000000")
    assert response.status_code == 404
