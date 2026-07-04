from __future__ import annotations

import httpx
import logging
from typing import Any

from app.application.shared.secret_manager_port import SecretManagerPort

logger = logging.getLogger(__name__)


class BaoSecretManagerAdapter(SecretManagerPort):
    """
    OpenBao (Vault) adapter using raw httpx for async HTTP resolution.
    Assumes KV v2 engine where data is nested under `data.data` or `data` directly
    if it's KV v1.
    """
    
    def __init__(self, vault_url: str, vault_token: str) -> None:
        self.vault_url = vault_url.rstrip("/")
        self.vault_token = vault_token
        
    async def resolve(self, ref: str) -> dict[str, str]:
        """
        Resolves a Vault/Bao reference to a credential dictionary.
        
        Args:
            ref: The vault path, e.g., 'secret/data/my/db'
            
        Raises:
            KeyError: If the secret is not found.
            RuntimeError: If vault communication fails.
        """
        url = f"{self.vault_url}/v1/{ref.lstrip('/')}"
        headers = {"X-Vault-Token": self.vault_token}
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, headers=headers, timeout=10.0)
            except httpx.RequestError as e:
                logger.error(f"Error communicating with OpenBao: {e}")
                raise RuntimeError(f"OpenBao connection failed: {e}") from e

        if response.status_code == 404:
            raise KeyError(f"Secret not found at ref: {ref}")
            
        if response.status_code != 200:
            logger.error(f"OpenBao returned status {response.status_code}: {response.text}")
            raise RuntimeError(f"OpenBao request failed with status {response.status_code}")
            
        payload = response.json()
        
        # KV v2 returns data inside data.data
        if "data" in payload and "data" in payload["data"]:
            data = payload["data"]["data"]
        # KV v1 returns data inside data directly
        elif "data" in payload:
            data = payload["data"]
        else:
            data = payload
            
        # Ensure it returns dict[str, str]
        return {str(k): str(v) for k, v in data.items()}
