# app/infrastructure/adapters/platform/_base_client.py
from __future__ import annotations

import httpx


class BasePlatformClient:
    """Encapsula configuracao base de HTTP para clientes de plataforma."""

    def __init__(
        self,
        base_url: str | None = None,
        vault_url: str | None = None,
        vault_token: str | None = None,
        timeout: float = 10.0,
    ) -> None:
        self._base_url = (base_url or "").rstrip("/")
        self._vault_url = vault_url
        self._vault_token = vault_token
        self._timeout = timeout

    def _get_client(self) -> httpx.Client:
        return httpx.Client(base_url=self._base_url, timeout=self._timeout)
