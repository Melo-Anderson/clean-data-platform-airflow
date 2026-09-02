from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine

from app.infrastructure.persistence.database import get_engine, get_session_factory


def test_get_engine_returns_async_engine() -> None:
    engine = get_engine()
    assert isinstance(engine, AsyncEngine)


def test_get_session_factory_returns_sessionmaker() -> None:
    factory = get_session_factory()
    assert factory is not None
    assert callable(factory)
