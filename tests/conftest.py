from __future__ import annotations

import os
import time
from collections.abc import AsyncGenerator

from hypothesis import HealthCheck, settings

settings.register_profile("dev", max_examples=50, suppress_health_check=[HealthCheck.too_slow])
settings.register_profile("ci", max_examples=500, suppress_health_check=[HealthCheck.too_slow])
settings.load_profile(os.getenv("HYPOTHESIS_PROFILE", "dev"))

import jwt as pyjwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

if "PLATFORM_DATABASE_URL" not in os.environ:
    os.environ[
        "PLATFORM_DATABASE_URL"
    ] = "sqlite+aiosqlite:///file:testdb?mode=memory&cache=shared&uri=true"
os.environ["PLATFORM_SECRET_KEY"] = "test"

from app.config import get_settings
from app.infrastructure.persistence.database import _engine, get_db
from app.main import create_app


@pytest.fixture(scope="session")
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


@pytest.fixture(scope="session")
async def engine():
    # Use the same engine created by database.py so we don't have connection locking issues
    yield _engine


@pytest.fixture(autouse=True)
async def setup_tables(engine, rsa_keypair):
    if os.getenv("API_URL"):
        yield
        return

    _, public_pem = rsa_keypair
    get_settings.cache_clear()
    settings = get_settings()
    settings.auth_jwt_public_key_pem = public_pem

    from app.infrastructure.persistence.base_model import Base
    from app.infrastructure.persistence.database import get_session_factory
    from scripts.init_db import seed_rbac

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with get_session_factory()() as session:
        await seed_rbac(session)

    yield
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture
async def db_session(engine) -> AsyncGenerator[AsyncSession, None]:
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        yield session


@pytest.fixture
def app(db_session):
    application = create_app()

    async def override_get_db():
        yield db_session

    application.dependency_overrides[get_db] = override_get_db
    return application


@pytest.fixture
async def client(app, rsa_keypair) -> AsyncGenerator[AsyncClient, None]:
    private_pem, _ = rsa_keypair
    token = _get_token(private_pem, "sre")
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {token}"},
    ) as ac:
        yield ac


def _get_token(private_pem: str, role: str) -> str:
    payload = {
        "sub": "u1",
        "email": f"{role}@co.com",
        "roles": [role],
        "exp": int(time.time()) + 300,
    }
    return pyjwt.encode(payload, private_pem, algorithm="RS256")


@pytest.fixture
async def ae_client(app, rsa_keypair) -> AsyncGenerator[AsyncClient, None]:
    private_pem, _ = rsa_keypair
    token = _get_token(private_pem, "analytics_engineer")
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {token}"},
    ) as ac:
        yield ac


@pytest.fixture
async def sre_client(app, rsa_keypair) -> AsyncGenerator[AsyncClient, None]:
    private_pem, _ = rsa_keypair
    token = _get_token(private_pem, "sre")
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {token}"},
    ) as ac:
        yield ac


@pytest.fixture
async def po_pm_client(app, rsa_keypair) -> AsyncGenerator[AsyncClient, None]:
    private_pem, _ = rsa_keypair
    token = _get_token(private_pem, "po_pm")
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {token}"},
    ) as ac:
        yield ac
