from __future__ import annotations

import httpx
import pytest
from unittest.mock import AsyncMock, patch

from app.infrastructure.adapters.secrets.bao_secret_manager_adapter import (
    BaoSecretManagerAdapter,
)


@pytest.mark.asyncio
async def test_bao_adapter_raises_runtime_error_when_vault_unreachable() -> None:
    """Vault inacessível deve levantar RuntimeError após esgotar retries."""
    adapter = BaoSecretManagerAdapter(vault_url="http://vault:8200", vault_token="root")

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=httpx.ConnectError("refused"))
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        with pytest.raises(RuntimeError, match="OpenBao connection failed"):
            await adapter.resolve("secret/postgres")


@pytest.mark.asyncio
async def test_bao_adapter_raises_key_error_on_404() -> None:
    """Secret não encontrado deve levantar KeyError imediatamente sem retry."""
    adapter = BaoSecretManagerAdapter(vault_url="http://vault:8200", vault_token="root")
    mock_response = AsyncMock(status_code=404)

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        with pytest.raises(KeyError, match="secret/postgres"):
            await adapter.resolve("secret/postgres")
