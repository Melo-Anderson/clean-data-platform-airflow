from __future__ import annotations

from app.domain.endpoints.endpoint import Endpoint
from app.domain.endpoints.endpoint_repository import EndpointRepository


class EndpointNotFoundError(Exception):
    def __init__(self, endpoint_id: str) -> None:
        super().__init__(f"Endpoint not found: id={endpoint_id!r}")
        self.endpoint_id = endpoint_id


class EndpointService:
    """
    Domain service for Endpoint provisioning.

    Accepts a fully-constructed typed Endpoint subclass.
    Subtype-specific validation is done by the subclass itself
    (e.g., non-empty host) or in the HTTP schema layer.
    """

    def __init__(self, repo: EndpointRepository) -> None:
        self._repo = repo

    async def provision(self, endpoint: Endpoint) -> Endpoint:
        """
        Persist a pre-built typed Endpoint and return the saved entity.

        Example:
            ep = DatabaseEndpoint(id="uuid", ..., host="oracle.internal", port=1521, ...)
            saved = await service.provision(ep)
        """
        return await self._repo.save(endpoint)

    async def find_for_asset(self, asset_id: str) -> Endpoint | None:
        """Return the Endpoint linked to a DataAsset, or None if not provisioned yet."""
        return await self._repo.find_by_asset_id(asset_id)
