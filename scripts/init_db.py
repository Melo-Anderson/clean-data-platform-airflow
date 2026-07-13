from __future__ import annotations

import asyncio

from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.persistence.base_model import Base
from app.infrastructure.persistence.database import _engine
from app.infrastructure.persistence.models import (  # noqa: F401 — ensures models are registered
    PermissionModel,
    RoleModel,
    RolePermissionModel,
)

_SEED_DATA: dict[str, list[str]] = {
    "sre": [
        "pipeline:create",
        "pipeline:delete",
        "pipeline:trigger",
        "pipeline:view",
        "drift:approve",
        "drift:view",
        "catalog:view",
        "catalog:sync",
    ],
    "analytics_engineer": [
        "pipeline:view",
        "pipeline:create",
        "pipeline:delete",
        "pipeline:trigger",
        "catalog:view",
        "catalog:edit",
        "catalog:sync",
    ],
    "po_pm": ["pipeline:view", "drift:approve", "drift:view", "catalog:view"],
}


async def seed_rbac(session: AsyncSession) -> None:
    """Idempotently seed roles and permissions into the database."""
    from sqlalchemy import select

    # collect all unique permissions
    all_perms = sorted({p for perms in _SEED_DATA.values() for p in perms})

    # upsert permissions
    for perm_name in all_perms:
        exists = (
            await session.execute(select(PermissionModel).where(PermissionModel.name == perm_name))
        ).scalar_one_or_none()
        if not exists:
            session.add(PermissionModel(name=perm_name))
    await session.flush()

    # upsert roles and links
    for role_name, perm_names in _SEED_DATA.items():
        role = (
            await session.execute(select(RoleModel).where(RoleModel.name == role_name))
        ).scalar_one_or_none()
        if not role:
            role = RoleModel(name=role_name)
            session.add(role)
            await session.flush()

        for perm_name in perm_names:
            perm = (
                await session.execute(
                    select(PermissionModel).where(PermissionModel.name == perm_name)
                )
            ).scalar_one()
            link_exists = (
                await session.execute(
                    select(RolePermissionModel).where(
                        RolePermissionModel.role_id == role.id,
                        RolePermissionModel.permission_id == perm.id,
                    )
                )
            ).scalar_one_or_none()
            if not link_exists:
                session.add(RolePermissionModel(role_id=role.id, permission_id=perm.id))

    await session.commit()


async def init_db() -> None:
    from app.infrastructure.persistence.database import get_session_factory

    print("Creating database tables...")
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("Tables created successfully!")

    factory = get_session_factory()
    async with factory() as session:
        print("Seeding RBAC roles and permissions...")
        await seed_rbac(session)
        print("Seed complete!")


if __name__ == "__main__":
    asyncio.run(init_db())
