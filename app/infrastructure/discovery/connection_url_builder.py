# app/infrastructure/discovery/connection_url_builder.py
from __future__ import annotations


def build_connection_url(payload: dict[str, str]) -> str:
    """
    Assemble a SQLAlchemy-compatible connection URL from a Vault credential payload.

    The payload is a flat dict as returned by SecretManagerPort.resolve().
    Keys: driver (required), database (required), host, port, user, password (all optional).

    SQLite special case:
        {"driver": "sqlite+aiosqlite", "database": ":memory:"}
        → "sqlite+aiosqlite:///:memory:"

    Raises:
        ValueError: if 'driver' or 'database' are missing from the payload.
    """
    driver = payload.get("driver")
    if not driver:
        raise ValueError("Vault payload missing required key 'driver'.")

    database = payload.get("database")
    if not database:
        raise ValueError("Vault payload missing required key 'database'.")

    # SQLite uses a file-path format: sqlite+aiosqlite:///path/to/file or /:memory:
    if driver.startswith("sqlite"):
        return f"{driver}:///{database}"

    host = payload.get("host", "")
    port = payload.get("port", "")
    user = payload.get("user", "")
    password = payload.get("password", "")

    auth = ""
    if user and password:
        auth = f"{user}:{password}@"
    elif user:
        auth = f"{user}@"

    netloc = f"{host}:{port}" if port else host

    return f"{driver}://{auth}{netloc}/{database}"
