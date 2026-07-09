from __future__ import annotations

import os

import pytest
from httpx import ASGITransport, AsyncClient
from collections.abc import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.auth.current_user import CurrentUser
from app.auth.dependencies import get_current_user
from app.auth.role import Role
from app.domain.shared.value_objects import EmailAddress

if not os.getenv("API_URL"):
    os.environ["PLATFORM_DATABASE_URL"] = (
        "sqlite+aiosqlite:///file:testdb?mode=memory&cache=shared&uri=true"
    )
os.environ["PLATFORM_SECRET_KEY"] = "test"


from app.infrastructure.persistence.database import _engine, get_db
from app.main import create_app


@pytest.fixture(scope="session")
async def engine():
    # Use the same engine created by database.py so we don't have connection locking issues
    yield _engine
    # Do not dispose because tests might still run or it's global


@pytest.fixture(autouse=True)
async def setup_tables(engine):
    if os.getenv("API_URL"):
        yield
        return

    from app.infrastructure.persistence.base_model import Base

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
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
async def client(app) -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac


def _override(role: Role):
    user = CurrentUser(id="u1", email=EmailAddress("test@co.com"), role=role)
    return lambda: user


@pytest.fixture
async def ae_client(app, client: AsyncClient) -> AsyncClient:
    app.dependency_overrides[get_current_user] = _override(Role.ANALYTICS_ENGINEER)
    return client


@pytest.fixture
async def sre_client(app, client: AsyncClient) -> AsyncClient:
    app.dependency_overrides[get_current_user] = _override(Role.SRE)
    return client


@pytest.fixture
async def po_pm_client(app, client: AsyncClient) -> AsyncClient:
    app.dependency_overrides[get_current_user] = _override(Role.PO_PM)
    return client
