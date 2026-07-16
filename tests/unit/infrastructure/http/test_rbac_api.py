import pytest
from httpx import AsyncClient

from app.auth.dependencies import get_jwt_validator, get_permission_resolver
from app.main import app


class MockForbiddenResolver:
    async def get_permissions_for_roles(self, roles: list[str]) -> set[str]:
        return {"catalog:view"}  # Missing "catalog:edit" required for asset creation


class MockValidator:
    def validate(self, token: str) -> dict:
        return {"sub": "1"}

    def extract_roles(self, payload: dict) -> list[str]:
        return ["reader"]


@pytest.mark.asyncio
async def test_rbac_asset_creation_unauthorized():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.post(
            "/v1/assets/",
            json={"name": "test_asset", "owner_email": "test@platform.local"},
        )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_rbac_asset_creation_forbidden():
    app.dependency_overrides[get_jwt_validator] = lambda: MockValidator()
    app.dependency_overrides[get_permission_resolver] = lambda: MockForbiddenResolver()

    try:
        async with AsyncClient(app=app, base_url="http://test") as ac:
            response = await ac.post(
                "/v1/assets/",
                json={"name": "test_asset", "owner_email": "test@platform.local"},
                headers={"Authorization": "Bearer mocked_token"},
            )

        assert response.status_code == 403
    finally:
        app.dependency_overrides.clear()
