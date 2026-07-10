# tests/unit/infrastructure/discovery/test_connection_url_builder.py
from __future__ import annotations

import pytest

from app.infrastructure.discovery.connection_url_builder import build_connection_url


def test_build_url_full_postgres() -> None:
    payload = {
        "driver": "postgresql+asyncpg",
        "host": "db.internal",
        "port": "5432",
        "user": "svc",
        "password": "secret",
        "database": "analytics",
    }
    url = build_connection_url(payload)
    assert url == "postgresql+asyncpg://svc:secret@db.internal:5432/analytics"


def test_build_url_sqlite_in_memory() -> None:
    payload = {"driver": "sqlite+aiosqlite", "database": ":memory:"}
    url = build_connection_url(payload)
    assert url == "sqlite+aiosqlite:///:memory:"


def test_build_url_missing_driver_raises() -> None:
    with pytest.raises(ValueError, match="driver"):
        build_connection_url({"database": "mydb"})


def test_build_url_missing_database_raises() -> None:
    with pytest.raises(ValueError, match="database"):
        build_connection_url({"driver": "postgresql+asyncpg", "host": "localhost"})


def test_build_url_no_auth() -> None:
    payload = {
        "driver": "postgresql+asyncpg",
        "host": "localhost",
        "database": "mydb",
    }
    url = build_connection_url(payload)
    assert url == "postgresql+asyncpg://localhost/mydb"
