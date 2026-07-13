from __future__ import annotations

import time

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.auth.permission_resolver import DatabasePermissionResolver
from app.infrastructure.persistence.base_model import Base
from app.infrastructure.persistence.models.permission_model import PermissionModel
from app.infrastructure.persistence.models.role_model import RoleModel
from app.infrastructure.persistence.models.role_permission_model import RolePermissionModel


@pytest.fixture
async def session_factory():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    # seed minimal data
    async with factory() as s:
        sre = RoleModel(name="sre")
        perm_create = PermissionModel(name="pipeline:create")
        perm_view = PermissionModel(name="pipeline:view")
        s.add_all([sre, perm_create, perm_view])
        await s.flush()
        s.add_all([
            RolePermissionModel(role_id=sre.id, permission_id=perm_create.id),
            RolePermissionModel(role_id=sre.id, permission_id=perm_view.id),
        ])
        await s.commit()
    return factory


async def test_resolver_returns_correct_permissions(session_factory):
    resolver = DatabasePermissionResolver(session_factory, ttl_seconds=60)
    perms = await resolver.get_permissions_for_roles(["sre"])
    assert "pipeline:create" in perms
    assert "pipeline:view" in perms


async def test_resolver_unknown_role_returns_empty(session_factory):
    resolver = DatabasePermissionResolver(session_factory, ttl_seconds=60)
    perms = await resolver.get_permissions_for_roles(["unknown_role"])
    assert perms == set()


async def test_resolver_caches_result(session_factory):
    resolver = DatabasePermissionResolver(session_factory, ttl_seconds=60)
    perms1 = await resolver.get_permissions_for_roles(["sre"])
    perms2 = await resolver.get_permissions_for_roles(["sre"])
    assert perms1 == perms2
    assert resolver._cache_hits >= 1


async def test_resolver_cache_expires(session_factory):
    resolver = DatabasePermissionResolver(session_factory, ttl_seconds=0)
    await resolver.get_permissions_for_roles(["sre"])
    time.sleep(0.01)
    await resolver.get_permissions_for_roles(["sre"])
    assert resolver._cache_hits == 0  # expired each time
