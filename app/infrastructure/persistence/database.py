from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import get_settings


def _build_engine():
    settings = get_settings()
    return create_async_engine(
        str(settings.database_url),
        echo=settings.debug,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
    )


_engine = _build_engine()
_session_factory = async_sessionmaker(_engine, expire_on_commit=False)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency: yields a transactional AsyncSession."""
    async with _session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Return the session factory for SqlUnitOfWork construction."""
    return _session_factory
