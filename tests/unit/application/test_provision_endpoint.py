from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.application.endpoints.provision_endpoint import ProvisionEndpointUseCase
from app.domain.endpoints.endpoint import NoSqlEndpoint


@pytest.mark.asyncio
async def test_execute_nosql_returns_nosql_endpoint() -> None:
    mock_uow = AsyncMock()
    mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
    mock_uow.__aexit__ = AsyncMock(return_value=False)

    saved_ep = NoSqlEndpoint(
        id="ep-mongo-1",
        name="prod-mongo",
        credential_ref=MagicMock(path="vault/mongo/prod"),
    )
    mock_uow.endpoints = AsyncMock()
    mock_service = MagicMock()
    mock_service.provision = AsyncMock(return_value=saved_ep)

    with patch(
        "app.application.endpoints.provision_endpoint.EndpointService", return_value=mock_service
    ):
        use_case = ProvisionEndpointUseCase(uow=mock_uow)
        result = await use_case.execute_nosql(
            name="prod-mongo",
            credential_ref="vault/mongo/prod",
            technical_description="MongoDB production cluster",
        )

    assert isinstance(result, NoSqlEndpoint)
