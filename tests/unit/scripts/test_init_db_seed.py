from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.infrastructure.persistence.base_model import Base
from app.infrastructure.persistence.models import RoleModel, PermissionModel, RolePermissionModel
from scripts.init_db import seed_rbac


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


async def test_seed_creates_all_roles(session):
    await seed_rbac(session)
    from sqlalchemy import select, func
    result = await session.execute(select(func.count()).select_from(RoleModel))
    assert result.scalar() == 3  # sre, analytics_engineer, po_pm


async def test_seed_sre_has_all_permissions(session):
    await seed_rbac(session)
    from sqlalchemy import select
    sre = (await session.execute(select(RoleModel).where(RoleModel.name == "sre"))).scalar_one()
    links = (await session.execute(
        select(RolePermissionModel).where(RolePermissionModel.role_id == sre.id)
    )).scalars().all()
    assert len(links) == 8


async def test_seed_is_idempotent(session):
    await seed_rbac(session)
    await seed_rbac(session)  # second call must not raise
    from sqlalchemy import select, func
    result = await session.execute(select(func.count()).select_from(RoleModel))
    assert result.scalar() == 3
