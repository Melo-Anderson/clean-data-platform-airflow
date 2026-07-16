import pytest
from fastapi.security import HTTPAuthorizationCredentials

from app.auth.dependencies import require_permission
from app.domain.shared.exceptions import PlatformForbiddenError


class MockJwtValidator:
    def __init__(self, payload: dict, roles: list[str]):
        self._payload = payload
        self._roles = roles

    def validate(self, token: str) -> dict:
        return self._payload

    def extract_roles(self, payload: dict) -> list[str]:
        return self._roles


class MockDatabasePermissionResolver:
    def __init__(self, permissions: set[str]):
        self._permissions = permissions

    async def get_permissions_for_roles(self, roles: list[str]) -> set[str]:
        return self._permissions


@pytest.mark.asyncio
async def test_require_permission_success():
    validator = MockJwtValidator(
        payload={"sub": "user123", "email": "test@local"}, roles=["role_a"]
    )
    resolver = MockDatabasePermissionResolver(permissions={"catalog:view"})

    enforce = require_permission("catalog:view")
    credentials = HTTPAuthorizationCredentials(scheme="Bearer", credentials="token")

    user = await enforce(credentials=credentials, _v=validator, _r=resolver)
    assert user.id == "user123"
    assert "role_a" in user.roles


@pytest.mark.asyncio
async def test_require_permission_forbidden():
    validator = MockJwtValidator(payload={"sub": "user123"}, roles=["role_b"])
    resolver = MockDatabasePermissionResolver(permissions={"catalog:edit"})

    enforce = require_permission("catalog:view")
    credentials = HTTPAuthorizationCredentials(scheme="Bearer", credentials="token")

    with pytest.raises(PlatformForbiddenError):
        await enforce(credentials=credentials, _v=validator, _r=resolver)
