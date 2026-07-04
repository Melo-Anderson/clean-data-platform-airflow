# app/application/shared/secret_manager_port.py
from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class SecretManagerPort(Protocol):
    """
    Port for resolving a credential reference path to a credential payload dict.

    The returned dict contains raw key-value pairs as stored in the secret manager
    (e.g., Vault). It is the caller's responsibility to interpret the dict and
    build the appropriate connection mechanism.

    Example:
        port = SomeAdapter(...)
        creds = await port.resolve("vault/db/prod")
        # creds == {"driver": "postgresql+asyncpg", "host": "...", "port": "5432",
        #           "user": "svc_acct", "password": "...", "database": "analytics"}
    """

    async def resolve(self, ref: str) -> dict[str, str]: ...
