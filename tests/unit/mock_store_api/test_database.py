import inspect

from services.mock_store_api.database import Base, engine, get_db


def test_get_db_is_async_generator():
    assert inspect.isasyncgenfunction(get_db)


def test_base_is_declarative():
    from sqlalchemy.orm import DeclarativeBase

    assert issubclass(Base, DeclarativeBase)


def test_engine_is_not_none():
    assert engine is not None
