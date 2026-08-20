from __future__ import annotations

from app.domain.endpoints.endpoint import AnyEndpoint
from app.domain.endpoints.endpoint_repository import EndpointRepository
from app.domain.shared.exceptions import PlatformNotFoundError


class EndpointNotFoundError(PlatformNotFoundError):
    def __init__(self, endpoint_id: str) -> None:
        super().__init__(f"Endpoint not found: id={endpoint_id!r}")
        self.endpoint_id = endpoint_id


class EndpointService:
    """
    Domain service for Endpoint provisioning.
    """

    def __init__(self, repo: EndpointRepository) -> None:
        self._repo = repo

    async def provision(self, endpoint: AnyEndpoint) -> AnyEndpoint:
        return await self._repo.save(endpoint)
