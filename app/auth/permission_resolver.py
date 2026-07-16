from __future__ import annotations

import time

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.infrastructure.persistence.models.permission_model import PermissionModel
from app.infrastructure.persistence.models.role_model import RoleModel
from app.infrastructure.persistence.models.role_permission_model import RolePermissionModel


class DatabasePermissionResolver:
    """Resolves role→permission mappings from the database with TTL-based in-memory caching."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession], ttl_seconds: int) -> None:
        self._session_factory = session_factory
        self._ttl = ttl_seconds
        self._cache: dict[str, tuple[set[str], float]] = {}
        self._cache_hits = 0

    async def get_permissions_for_roles(self, roles: list[str]) -> set[str]:
        """Return the union of all permissions assigned to the given roles."""
        key = ",".join(sorted(roles))
        now = time.monotonic()
        cached = self._cache.get(key)
        if cached is not None and (now - cached[1]) < self._ttl:
            self._cache_hits += 1
            return cached[0]

        self._cache_hits = 0
        permissions = await self._query_permissions(roles)
        self._cache[key] = (permissions, now)
        return permissions

    def invalidate_cache(self, roles: list[str] | None = None) -> None:
        """Invalidate permissions cache for specific roles, or all cache if none provided."""
        if roles is None:
            self._cache.clear()
        else:
            key = ",".join(sorted(roles))
            self._cache.pop(key, None)

    async def _query_permissions(self, roles: list[str]) -> set[str]:
        async with self._session_factory() as session:
            stmt = (
                select(PermissionModel.name)
                .join(RolePermissionModel, RolePermissionModel.permission_id == PermissionModel.id)
                .join(RoleModel, RoleModel.id == RolePermissionModel.role_id)
                .where(RoleModel.name.in_(roles))
            )
            result = await session.execute(stmt)
            return {row[0] for row in result.fetchall()}
