# tests/unit/infrastructure/discovery/test_connection_url_builder.py
from __future__ import annotations

from app.infrastructure.discovery.connection_url_builder import build_connection_url


def test_build_url_postgres_standard() -> None:
    payload = {
        "driver": "postgres",
        "host": "db.corp",
        "port": "5432",
        "user": "prod_user",
        "password": "secret_password",
        "database": "corp_db",
    }
    url = build_connection_url(payload, async_driver=False)
    assert url == "postgresql://prod_user:secret_password@db.corp:5432/corp_db"


def test_build_url_postgres_async() -> None:
    payload = {
        "driver": "postgres",
        "host": "db.corp",
        "port": "5432",
        "user": "prod_user",
        "password": "secret_password",
        "database": "corp_db",
        "sslmode": "disable",
    }
    url = build_connection_url(payload, async_driver=True)
    assert url == "postgresql+asyncpg://prod_user:secret_password@db.corp:5432/corp_db"

    payload_require = dict(payload, sslmode="require")
    assert (
        build_connection_url(payload_require, async_driver=True)
        == "postgresql+asyncpg://prod_user:secret_password@db.corp:5432/corp_db?ssl=require"
    )


def test_build_url_sqlite_standard_and_async() -> None:
    payload = {"driver": "sqlite", "database": ":memory:"}
    assert build_connection_url(payload, async_driver=False) == "sqlite:///:memory:"
    assert build_connection_url(payload, async_driver=True) == "sqlite+aiosqlite:///:memory:"


def test_build_url_mysql_standard_and_async() -> None:
    payload = {
        "driver": "mysql",
        "host": "localhost",
        "port": "3306",
        "user": "root",
        "password": "pass",
        "database": "inventory",
        "charset": "utf8mb4",
    }
    assert (
        build_connection_url(payload, async_driver=False)
        == "mysql://root:pass@localhost:3306/inventory?charset=utf8mb4"
    )
    assert (
        build_connection_url(payload, async_driver=True)
        == "mysql+aiomysql://root:pass@localhost:3306/inventory?charset=utf8mb4"
    )


def test_build_url_mongodb_with_auth_source() -> None:
    payload = {
        "driver": "mongodb",
        "host": "mongo-host",
        "port": "27017",
        "user": "mongo_user",
        "password": "secret_pass",
        "database": "analytics",
        "auth_source": "custom_auth",
    }
    url = build_connection_url(payload)
    assert (
        url == "mongodb://mongo_user:secret_pass@mongo-host:27017/analytics?authSource=custom_auth"
    )


def test_build_url_escapes_special_characters_in_credentials() -> None:
    payload = {
        "driver": "postgres",
        "host": "localhost",
        "port": "5432",
        "user": "user@domain.com",
        "password": "p@ss:w/ord?#",
        "database": "app_db",
    }
    url = build_connection_url(payload, async_driver=False)
    assert url == "postgresql://user%40domain.com:p%40ss%3Aw%2Ford%3F%23@localhost:5432/app_db"


def test_build_url_returns_connection_uri_if_present() -> None:
    payload = {
        "connection_uri": "postgresql://custom_user:custom_pass@my-vault-db:5432/app_db?sslmode=require",
        "driver": "postgres",
    }
    url = build_connection_url(payload)
    assert url == "postgresql://custom_user:custom_pass@my-vault-db:5432/app_db?sslmode=require"


def test_build_url_generic_fallback() -> None:
    payload = {
        "driver": "custom_driver",
        "host": "custom_host",
        "port": "9999",
        "user": "u",
        "password": "p",
        "database": "custom_db",
    }
    url = build_connection_url(payload)
    assert url == "custom_driver://u:p@custom_host:9999/custom_db"
