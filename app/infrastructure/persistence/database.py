from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from app.config import get_settings


def _build_engine() -> AsyncEngine:
    settings = get_settings()
    url = str(settings.database_url)
    kwargs: dict[str, Any] = {"echo": settings.debug}
    if not url.startswith("sqlite"):
        kwargs["pool_pre_ping"] = True
        kwargs["pool_size"] = 10
        kwargs["max_overflow"] = 20
    else:
        kwargs["poolclass"] = StaticPool

    return create_async_engine(url, **kwargs)


_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        _engine = _build_engine()
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(get_engine(), expire_on_commit=False)
    return _session_factory


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency: yields a transactional AsyncSession."""
    factory = get_session_factory()
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


def __getattr__(name: str) -> Any:
    if name == "_engine":
        return get_engine()
    if name == "_session_factory":
        return get_session_factory()
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
