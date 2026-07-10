# app/application/endpoints/provision_endpoint.py
from __future__ import annotations

from app.application.unit_of_work import UnitOfWork
from app.domain.endpoints.endpoint import AnyEndpoint, DatabaseEndpoint
from app.domain.endpoints.endpoint_service import EndpointService
from app.domain.shared.value_objects import CredentialReference


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

    async def execute_database(
        self, name: str, credential_ref: str, technical_description: str
    ) -> DatabaseEndpoint:
        import uuid

        ep = DatabaseEndpoint(
            id=str(uuid.uuid4()),
            name=name,
            credential_ref=CredentialReference(credential_ref),
            technical_description=technical_description,
        )
        from typing import cast

        async with self._uow:
            service = EndpointService(repo=self._uow.endpoints)
            saved = await service.provision(ep)
            await self._uow.commit()
        return cast(DatabaseEndpoint, saved)
