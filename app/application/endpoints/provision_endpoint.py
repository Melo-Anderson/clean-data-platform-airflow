# app/application/endpoints/provision_endpoint.py
from __future__ import annotations

from app.application.unit_of_work import UnitOfWork
from app.domain.endpoints.endpoint import AnyEndpoint
from app.domain.endpoints.endpoint_service import EndpointService


class ProvisionEndpointUseCase:
    """
    Provisions a new Endpoint within a UoW transaction.
    """

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def execute(self, endpoint: AnyEndpoint) -> AnyEndpoint:
        async with self._uow:
            service = EndpointService(repo=self._uow.endpoints)
            saved = await service.provision(endpoint)
            await self._uow.commit()
        return saved
