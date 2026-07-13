from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.infrastructure.persistence.base_model import Base
from app.infrastructure.persistence.models.permission_model import PermissionModel
from app.infrastructure.persistence.models.role_model import RoleModel
from app.infrastructure.persistence.models.role_permission_model import RolePermissionModel


@pytest.fixture
async def session():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as s:
        yield s
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


async def test_create_role_and_permission(session: AsyncSession):
    role = RoleModel(name="sre")
    perm = PermissionModel(name="pipeline:create")
    session.add_all([role, perm])
    await session.flush()
    link = RolePermissionModel(role_id=role.id, permission_id=perm.id)
    session.add(link)
    await session.commit()

    from sqlalchemy import select

    result = await session.execute(select(RoleModel).where(RoleModel.name == "sre"))
    loaded = result.scalar_one()
    assert loaded.name == "sre"
