# tests/unit/auth/test_role_dependency.py
from __future__ import annotations

import pytest
from fastapi import Depends, FastAPI
from httpx import ASGITransport, AsyncClient

from app.auth.current_user import CurrentUser
from app.auth.dependencies import get_current_user, require_role
from app.auth.role import Role
from app.domain.shared.value_objects import EmailAddress


def _user(role: Role) -> CurrentUser:
    return CurrentUser(id="u1", email=EmailAddress("test@co.com"), role=role)


def _make_app(required_role: Role) -> FastAPI:
    app = FastAPI()

    @app.get("/protected")
    async def _route(user: CurrentUser = Depends(require_role(required_role))) -> dict[str, str]:
        return {"role": user.role}

    return app


@pytest.mark.asyncio
async def test_matching_role_is_allowed() -> None:
    app = _make_app(Role.SRE)
    app.dependency_overrides[get_current_user] = lambda: _user(Role.SRE)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        response = await c.get("/protected", headers={"Authorization": "Bearer fake"})
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_wrong_role_is_rejected_with_403() -> None:
    app = _make_app(Role.SRE)
    app.dependency_overrides[get_current_user] = lambda: _user(Role.ANALYTICS_ENGINEER)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        response = await c.get("/protected", headers={"Authorization": "Bearer fake"})
    assert response.status_code == 403
    assert "analytics_engineer" in response.json()["detail"]
