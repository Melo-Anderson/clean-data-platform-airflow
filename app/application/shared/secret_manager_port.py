# app/application/shared/secret_manager_port.py
from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class SecretManagerPort(Protocol):
    """Port for resolving a secret reference path to a credential payload.

    Implementors must be async-safe and must NOT cache credentials in memory.
    Each call should be treated as a fresh resolution from the secret store.

    Example:
        adapter = BaoSecretManagerAdapter(vault_url="http://vault:8200", vault_token="root")
        creds = await adapter.resolve("secret/postgres/prod")
        # creds == {"host": "db.internal", "port": "5432", "user": "svc", "password": "***"}

    Raises:
        KeyError: If the secret path does not exist in the store.
        RuntimeError: If the secret store is unreachable after retries.
    """

    async def resolve(self, ref: str) -> dict[str, str]:
        """Resolve a secret reference to a flat credential dictionary.

        Args:
            ref: Secret store path. Format depends on the backend
                 (e.g. 'secret/my/db' for Vault KV v2).

        Returns:
            Flat dict mapping credential key names to their string values.
        """
        ...
