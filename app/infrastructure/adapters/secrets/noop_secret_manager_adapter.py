# app/infrastructure/adapters/secrets/noop_secret_manager_adapter.py
from __future__ import annotations


class NoopSecretManagerAdapter:
    """
    In-memory stub for SecretManagerPort. For local dev and tests only.

    Accepts a pre-loaded store dict so tests can inject any credential payload
    without side effects.

    Example:
        adapter = NoopSecretManagerAdapter(
            {"vault/db/test": {"driver": "sqlite+aiosqlite", "database": ":memory:"}}
        )
    """

    def __init__(self, store: dict[str, dict[str, str]] | None = None) -> None:
        self._store: dict[str, dict[str, str]] = (
            store
            if store is not None
            else {"secret": {"driver": "sqlite+aiosqlite", "database": ":memory:"}}
        )

    async def resolve(self, ref: str) -> dict[str, str]:
        if ref not in self._store:
            raise KeyError(f"No credential found for ref: {ref!r}")
        return dict(self._store[ref])
