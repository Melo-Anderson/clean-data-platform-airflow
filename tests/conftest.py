from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# The Base model and get_db will be implemented in later steps. We will comment them out for now to avoid import errors.
# from platform.infrastructure.persistence.base_model import Base
# from platform.infrastructure.persistence.database import get_db
from platform.main import create_app

TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"


@pytest.fixture(scope="session")
async def engine():
    eng = create_async_engine(TEST_DATABASE_URL, echo=False)
    yield eng
    await eng.dispose()


# @pytest.fixture(autouse=True)
# async def setup_tables(engine):
#     async with engine.begin() as conn:
#         await conn.run_sync(Base.metadata.create_all)
#     yield
#     async with engine.begin() as conn:
#         await conn.run_sync(Base.metadata.drop_all)


# @pytest.fixture
# async def db_session(engine) -> AsyncSession:
#     factory = async_sessionmaker(engine, expire_on_commit=False)
#     async with factory() as session:
#         yield session


@pytest.fixture
async def client() -> AsyncClient:
    app = create_app()

    # async def override_get_db():
    #     yield db_session

    # app.dependency_overrides[get_db] = override_get_db
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac
