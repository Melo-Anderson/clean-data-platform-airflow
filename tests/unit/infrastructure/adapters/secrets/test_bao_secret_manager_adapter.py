import pytest
import httpx
from typing import AsyncGenerator
from unittest.mock import patch, MagicMock

from app.infrastructure.adapters.secrets.bao_secret_manager_adapter import BaoSecretManagerAdapter


@pytest.fixture
def bao_adapter() -> BaoSecretManagerAdapter:
    return BaoSecretManagerAdapter(vault_url="http://localhost:8200", vault_token="root")


@pytest.mark.asyncio
async def test_resolve_success_kv2(bao_adapter: BaoSecretManagerAdapter) -> None:
    ref = "secret/data/my/db"

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "data": {"data": {"driver": "postgres", "host": "localhost"}}
    }

    with patch("httpx.AsyncClient.get", return_value=mock_response) as mock_get:
        creds = await bao_adapter.resolve(ref)

        assert creds == {"driver": "postgres", "host": "localhost"}
        mock_get.assert_called_once()
        args, kwargs = mock_get.call_args
        assert args[0] == "http://localhost:8200/v1/secret/data/my/db"
        assert kwargs["headers"] == {"X-Vault-Token": "root"}


@pytest.mark.asyncio
async def test_resolve_success_kv1(bao_adapter: BaoSecretManagerAdapter) -> None:
    ref = "secret/my/db"

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": {"driver": "postgres", "host": "localhost"}}

    with patch("httpx.AsyncClient.get", return_value=mock_response):
        creds = await bao_adapter.resolve(ref)
        assert creds == {"driver": "postgres", "host": "localhost"}


@pytest.mark.asyncio
async def test_resolve_not_found(bao_adapter: BaoSecretManagerAdapter) -> None:
    mock_response = MagicMock()
    mock_response.status_code = 404

    with patch("httpx.AsyncClient.get", return_value=mock_response):
        with pytest.raises(KeyError, match="Secret not found at ref"):
            await bao_adapter.resolve("secret/invalid")


@pytest.mark.asyncio
async def test_resolve_http_error(bao_adapter: BaoSecretManagerAdapter) -> None:
    with patch("httpx.AsyncClient.get", side_effect=httpx.RequestError("timeout")):
        with pytest.raises(RuntimeError, match="OpenBao connection failed"):
            await bao_adapter.resolve("secret/timeout")


@pytest.mark.asyncio
async def test_resolve_server_error(bao_adapter: BaoSecretManagerAdapter) -> None:
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"

    with patch("httpx.AsyncClient.get", return_value=mock_response):
        with pytest.raises(RuntimeError, match="OpenBao request failed with status 500"):
            await bao_adapter.resolve("secret/error")
