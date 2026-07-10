# tests/unit/infrastructure/discovery/test_secret_manager_port.py
from __future__ import annotations

import pytest

from app.application.shared.secret_manager_port import SecretManagerPort
from app.infrastructure.adapters.secrets.noop_secret_manager_adapter import NoopSecretManagerAdapter


@pytest.mark.asyncio
async def test_noop_adapter_fulfills_port_protocol() -> None:
    adapter = NoopSecretManagerAdapter(
        store={
            "vault/db/prod": {
                "driver": "sqlite+aiosqlite",
                "database": ":memory:",
            }
        }
    )
    assert isinstance(adapter, SecretManagerPort)
    result = await adapter.resolve("vault/db/prod")
    assert result["driver"] == "sqlite+aiosqlite"
    assert result["database"] == ":memory:"


@pytest.mark.asyncio
async def test_noop_adapter_raises_for_unknown_ref() -> None:
    adapter = NoopSecretManagerAdapter(store={})
    with pytest.raises(KeyError, match="vault/db/unknown"):
        await adapter.resolve("vault/db/unknown")
