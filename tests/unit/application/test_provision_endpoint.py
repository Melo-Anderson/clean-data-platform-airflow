from __future__ import annotations

import uuid
from typing import Any

import pytest

from app.application.endpoints.provision_endpoint import ProvisionEndpointUseCase
from app.domain.endpoints.endpoint import DatabaseEndpoint, NoSqlEndpoint, RestApiEndpoint
from app.domain.shared.value_objects import CredentialReference

# ---------------------------------------------------------------------------
# Mock nomeado — sem MagicMock anônimo (regra do projeto)
# ---------------------------------------------------------------------------


class StubEndpointRepo:
    """In-memory endpoint repository stub."""

    def __init__(self) -> None:
        self.saved: list[Any] = []

    async def save(self, endpoint: Any) -> Any:
        self.saved.append(endpoint)
        return endpoint

    async def find_by_id(self, endpoint_id: str) -> Any:
        return None

    async def find_by_name(self, name: str) -> Any:
        return None


class StubUoW:
    """In-memory UoW stub matching the UnitOfWork Protocol."""

    def __init__(self) -> None:
        self._endpoint_repo = StubEndpointRepo()
        self.committed = False

    @property
    def endpoints(self) -> StubEndpointRepo:
        return self._endpoint_repo

    async def commit(self) -> None:
        self.committed = True

    async def rollback(self) -> None:
        pass

    async def __aenter__(self) -> StubUoW:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        pass


@pytest.mark.asyncio
async def test_execute_provisions_database_endpoint() -> None:
    """execute() deve persistir um DatabaseEndpoint e commitar."""
    uow = StubUoW()
    ep = DatabaseEndpoint(
        id=str(uuid.uuid4()),
        name="prod-oracle",
        credential_ref=CredentialReference("vault/oracle/prod"),
    )
    saved = await ProvisionEndpointUseCase(uow=uow).execute(ep)
    assert saved.name == "prod-oracle"
    assert uow.committed is True
    assert len(uow.endpoints.saved) == 1


@pytest.mark.asyncio
async def test_execute_provisions_nosql_endpoint() -> None:
    """execute() deve persistir um NoSqlEndpoint e commitar."""
    uow = StubUoW()
    ep = NoSqlEndpoint(
        id=str(uuid.uuid4()),
        name="prod-mongo",
        credential_ref=CredentialReference("vault/mongo/prod"),
    )
    saved = await ProvisionEndpointUseCase(uow=uow).execute(ep)
    assert saved.name == "prod-mongo"
    assert uow.committed is True


@pytest.mark.asyncio
async def test_execute_provisions_rest_api_endpoint() -> None:
    """execute() deve persistir um RestApiEndpoint e commitar."""
    uow = StubUoW()
    ep = RestApiEndpoint(
        id=str(uuid.uuid4()),
        name="store-api",
        credential_ref=CredentialReference("vault/api/store"),
        base_url="https://api.store.com",
        auth_type="bearer",
    )
    saved = await ProvisionEndpointUseCase(uow=uow).execute(ep)
    assert saved.name == "store-api"
    assert uow.committed is True
