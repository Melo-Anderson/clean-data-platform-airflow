from __future__ import annotations

from app.application.unit_of_work import UnitOfWork
from app.domain.endpoints.endpoint import AnyEndpoint
from app.domain.endpoints.endpoint_service import EndpointService


class ProvisionEndpointUseCase:
    """
    Provisions a new Endpoint within a UoW transaction.

    Accepts any AnyEndpoint subtype. The HTTP layer is responsible for
    constructing the typed endpoint (DatabaseEndpoint, RestApiEndpoint, etc.)
    before calling execute(). This eliminates per-type helper methods and
    keeps the use case DRY.
    """

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def execute(self, endpoint: AnyEndpoint) -> AnyEndpoint:
        """Persist the endpoint in a single transactional boundary."""
        async with self._uow:
            service = EndpointService(repo=self._uow.endpoints)
            saved = await service.provision(endpoint)
            await self._uow.commit()
        return saved
