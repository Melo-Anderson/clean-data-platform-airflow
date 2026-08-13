from __future__ import annotations

import logging

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.application.shared.secret_manager_port import SecretManagerPort

logger = logging.getLogger(__name__)


def _normalize_vault_path(ref: str) -> str:
    """Insert 'data/' after the mount point if not already a KV v2 path."""
    ref_clean = ref.lstrip("/")
    parts = ref_clean.split("/", 1)
    if len(parts) == 2 and parts[1] != "data" and not parts[1].startswith("data/"):
        ref_clean = f"{parts[0]}/data/{parts[1]}"
    return ref_clean


def _unpack_vault_response(payload: dict) -> dict[str, str]:
    """Normalize KV v1 and KV v2 response envelopes to a flat credential dict."""
    if "data" in payload and "data" in payload["data"]:
        data = payload["data"]["data"]  # KV v2
    elif "data" in payload:
        data = payload["data"]  # KV v1
    else:
        data = payload
    return {str(k): str(v) for k, v in data.items()}


class BaoSecretManagerAdapter(SecretManagerPort):
    """
    OpenBao (Vault) adapter using raw httpx for async HTTP resolution.
    Assumes KV v2 engine where data is nested under `data.data` or `data` directly
    if it's KV v1.
    """

    def __init__(
        self,
        vault_url: str,
        vault_token: str,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.vault_url = vault_url.rstrip("/")
        self.vault_token = vault_token
        # When injected (e.g. in tests), reuse the long-lived client.
        # When None, a short-lived client is created per call via `async with`.
        self._client = http_client

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5),
        retry=retry_if_exception_type(RuntimeError),
        reraise=True,
    )
    async def resolve(self, ref: str) -> dict[str, str]:
        """
        Resolves a Vault/Bao reference to a credential dictionary.

        Args:
            ref: The vault path, e.g., 'secret/my/db' or 'secret/data/my/db'

        Raises:
            KeyError: If the secret is not found.
            RuntimeError: If vault communication fails.
        """
        ref_clean = _normalize_vault_path(ref)
        url = f"{self.vault_url}/v1/{ref_clean}"
        headers = {"X-Vault-Token": self.vault_token}

        if self._client is not None:
            return await self._make_request(self._client, url, headers, ref)

        async with httpx.AsyncClient() as client:
            return await self._make_request(client, url, headers, ref)

    async def _make_request(
        self, client: httpx.AsyncClient, url: str, headers: dict, ref: str
    ) -> dict[str, str]:
        try:
            response = await client.get(url, headers=headers, timeout=10.0)
        except httpx.RequestError as e:
            logger.error("Error communicating with OpenBao: %s", e)
            raise RuntimeError(f"OpenBao connection failed: {e}") from e

        if response.status_code == 404:
            raise KeyError(f"Secret not found at ref: {ref}")
        if response.status_code != 200:
            logger.error("OpenBao returned status %s: %s", response.status_code, response.text)
            raise RuntimeError(f"OpenBao request failed with status {response.status_code}")

        return _unpack_vault_response(response.json())
