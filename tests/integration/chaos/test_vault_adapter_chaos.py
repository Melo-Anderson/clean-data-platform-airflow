from __future__ import annotations

import httpx
import pytest
import respx

from app.infrastructure.adapters.secrets.bao_secret_manager_adapter import BaoSecretManagerAdapter

VAULT_URL = "http://fake-vault:8200"
SECRET_PATH = "secret/data/my-db"
VAULT_API_URL = f"{VAULT_URL}/v1/{SECRET_PATH}"


@pytest.fixture
def adapter() -> BaoSecretManagerAdapter:
    return BaoSecretManagerAdapter(vault_url=VAULT_URL, vault_token="root")


@pytest.mark.asyncio
@respx.mock
async def test_vault_resolve_succeeds(adapter: BaoSecretManagerAdapter) -> None:
    """When Vault returns 200 with KV v2 payload, resolve returns the secret dict."""
    respx.get(VAULT_API_URL).mock(
        return_value=httpx.Response(
            200,
            json={"data": {"data": {"user": "admin", "password": "secret123"}}},
        )
    )

    result = await adapter.resolve(SECRET_PATH)
    assert result == {"user": "admin", "password": "secret123"}


@pytest.mark.asyncio
@respx.mock
async def test_vault_returns_runtime_error_on_500(adapter: BaoSecretManagerAdapter) -> None:
    """Persistent 500 from Vault exhausts retries and raises RuntimeError."""
    respx.get(VAULT_API_URL).mock(return_value=httpx.Response(500, text="Internal Server Error"))

    with pytest.raises(RuntimeError, match="OpenBao request failed with status 500"):
        await adapter.resolve(SECRET_PATH)


@pytest.mark.asyncio
@respx.mock
async def test_vault_returns_key_error_on_404(adapter: BaoSecretManagerAdapter) -> None:
    """A 404 from Vault raises KeyError with the secret ref in the message."""
    respx.get(VAULT_API_URL).mock(return_value=httpx.Response(404))

    with pytest.raises(KeyError, match=SECRET_PATH):
        await adapter.resolve(SECRET_PATH)
