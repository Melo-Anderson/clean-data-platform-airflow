from __future__ import annotations

import time

import jwt as pyjwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.auth.dependencies import require_permission
from app.auth.jwt_validator import JwtValidator
from app.auth.permission_resolver import DatabasePermissionResolver
from app.config import Settings
from app.infrastructure.http.exception_handlers import register_exception_handlers
from app.infrastructure.persistence.base_model import Base
from app.infrastructure.persistence.models.permission_model import PermissionModel
from app.infrastructure.persistence.models.role_model import RoleModel
from app.infrastructure.persistence.models.role_permission_model import RolePermissionModel


@pytest.fixture(scope="module")
def rsa_keypair():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    ).decode()
    public_pem = (
        private_key.public_key()
        .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        .decode()
    )
    return private_pem, public_pem


@pytest.fixture
async def app_with_deps(rsa_keypair):
    private_pem, public_pem = rsa_keypair
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", poolclass=StaticPool)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async with factory() as s:
        sre = RoleModel(name="sre")
        perm = PermissionModel(name="pipeline:create")
        s.add_all([sre, perm])
        await s.flush()
        s.add(RolePermissionModel(role_id=sre.id, permission_id=perm.id))
        await s.commit()

    settings = Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        secret_key="test",
        auth_jwt_public_key_pem=public_pem,
    )
    validator = JwtValidator(settings)
    resolver = DatabasePermissionResolver(factory, ttl_seconds=60)

    from fastapi import Depends

    from app.auth.dependencies import get_jwt_validator, get_permission_resolver

    app = FastAPI()
    register_exception_handlers(app)
    app.dependency_overrides[get_jwt_validator] = lambda: validator
    app.dependency_overrides[get_permission_resolver] = lambda: resolver

    @app.get("/protected", dependencies=[Depends(require_permission("pipeline:create"))])
    async def protected():
        return {"ok": True}

    return app, private_pem


async def test_valid_token_with_permission_passes(app_with_deps):
    app, private_pem = app_with_deps
    token = pyjwt.encode(
        {"sub": "u1", "roles": ["sre"], "exp": int(time.time()) + 300},
        private_pem,
        algorithm="RS256",
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get("/protected", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200


async def test_valid_token_without_permission_returns_403(app_with_deps):
    app, private_pem = app_with_deps
    token = pyjwt.encode(
        {"sub": "u2", "roles": ["po_pm"], "exp": int(time.time()) + 300},
        private_pem,
        algorithm="RS256",
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get("/protected", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 403
    assert resp.json()["status"] == 403


async def test_no_token_returns_403(app_with_deps):
    app, _ = app_with_deps
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get("/protected")
    assert resp.status_code in (401, 403)
