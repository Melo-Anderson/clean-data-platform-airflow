# app/infrastructure/discovery/connection_url_builder.py
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any
from urllib.parse import quote_plus


class ConnectionUrlStrategy(ABC):
    @abstractmethod
    def build(self, payload: dict[str, Any], async_driver: bool) -> str:
        """Build connection URL for this database dialect."""
        pass

    @staticmethod
    def _auth_and_netloc(payload: dict[str, Any]) -> tuple[str, str]:
        user = payload.get("user", "")
        password = payload.get("password", "")
        auth = (
            f"{quote_plus(str(user))}:{quote_plus(str(password))}@"
            if user and password
            else f"{quote_plus(str(user))}@"
            if user
            else ""
        )
        host = payload.get("host", "")
        port = payload.get("port", "")
        netloc = f"{host}:{port}" if port else str(host)
        return auth, netloc


class PostgresUrlStrategy(ConnectionUrlStrategy):
    def build(self, payload: dict[str, Any], async_driver: bool) -> str:
        auth, netloc = self._auth_and_netloc(payload)
        database = payload.get("database", "")
        scheme = "postgresql+asyncpg" if async_driver else "postgresql"
        sslmode = str(payload.get("sslmode", "")).strip().lower()
        query = ""
        if sslmode:
            if async_driver:
                query = f"?ssl={sslmode}" if sslmode != "disable" else ""
            else:
                query = f"?sslmode={sslmode}"
        return f"{scheme}://{auth}{netloc}/{database}{query}"


class SqliteUrlStrategy(ConnectionUrlStrategy):
    def build(self, payload: dict[str, Any], async_driver: bool) -> str:
        database = payload.get("database", "")
        scheme = "sqlite+aiosqlite" if async_driver else "sqlite"
        return f"{scheme}:///{database}"


class MysqlUrlStrategy(ConnectionUrlStrategy):
    def build(self, payload: dict[str, Any], async_driver: bool) -> str:
        auth, netloc = self._auth_and_netloc(payload)
        database = payload.get("database", "")
        scheme = "mysql+aiomysql" if async_driver else "mysql"
        charset = payload.get("charset")
        query = f"?charset={charset}" if charset else ""
        return f"{scheme}://{auth}{netloc}/{database}{query}"


class MongoUrlStrategy(ConnectionUrlStrategy):
    def build(self, payload: dict[str, Any], async_driver: bool) -> str:
        auth, netloc = self._auth_and_netloc(payload)
        database = payload.get("database", "")
        auth_source = payload.get("auth_source", "admin")
        return f"mongodb://{auth}{netloc}/{database}?authSource={auth_source}"


class GenericUrlStrategy(ConnectionUrlStrategy):
    def build(self, payload: dict[str, Any], async_driver: bool) -> str:
        driver = payload.get("driver", "")
        auth, netloc = self._auth_and_netloc(payload)
        database = payload.get("database", "")
        return f"{driver}://{auth}{netloc}/{database}"


_STRATEGIES: dict[str, ConnectionUrlStrategy] = {
    "postgres": PostgresUrlStrategy(),
    "postgresql": PostgresUrlStrategy(),
    "postgresql+asyncpg": PostgresUrlStrategy(),
    "sqlite": SqliteUrlStrategy(),
    "sqlite+aiosqlite": SqliteUrlStrategy(),
    "mysql": MysqlUrlStrategy(),
    "mysql+aiomysql": MysqlUrlStrategy(),
    "mongodb": MongoUrlStrategy(),
    "mongo": MongoUrlStrategy(),
}

_GENERIC_STRATEGY = GenericUrlStrategy()


def build_connection_url(payload: dict[str, Any], *, async_driver: bool = False) -> str:
    """Build connection URL: returns 'connection_uri' directly if present, else dispatches to driver strategy."""
    if payload.get("connection_uri"):
        return str(payload["connection_uri"])

    driver = str(payload.get("driver", "")).lower()
    strategy = _STRATEGIES.get(driver, _GENERIC_STRATEGY)
    return strategy.build(payload, async_driver=async_driver)
