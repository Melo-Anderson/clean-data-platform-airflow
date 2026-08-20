from __future__ import annotations

from app.application.unit_of_work import UnitOfWork
from app.domain.endpoints.endpoint import AnyEndpoint


class ProvisionEndpointUseCase:
    """Provisions a new Endpoint directly within a UoW transaction."""

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def execute(self, endpoint: AnyEndpoint) -> AnyEndpoint:
        """Persist the endpoint in a single transactional boundary."""
        async with self._uow:
            saved = await self._uow.endpoints.save(endpoint)
            await self._uow.commit()
        return saved
