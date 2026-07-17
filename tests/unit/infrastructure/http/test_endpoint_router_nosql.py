from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app  # adjust if your app entry point differs


@pytest.fixture
def client():
    return TestClient(app)


def test_post_nosql_endpoint_returns_201(client) -> None:
    with (
        patch(
            "app.infrastructure.http.routers.endpoint_router.ProvisionEndpointUseCase"
        ) as MockUseCase,
        patch(
            "app.auth.dependencies.JwtValidator.validate",
            return_value={"sub": "test-user", "email": "test@example.com"},
        ),
        patch("app.auth.dependencies.JwtValidator.extract_roles", return_value=["SRE"]),
        patch(
            "app.auth.dependencies.DatabasePermissionResolver.get_permissions_for_roles",
            return_value=["catalog:sync"],
        ),
    ):
        from app.domain.endpoints.endpoint import NoSqlEndpoint
        from app.domain.shared.value_objects import CredentialReference

        mock_instance = AsyncMock()
        mock_instance.execute_nosql.return_value = NoSqlEndpoint(
            id="ep-mongo-1",
            name="prod-mongo",
            credential_ref=CredentialReference("vault/mongo/prod"),
        )
        MockUseCase.return_value = mock_instance

        response = client.post(
            "/v1/endpoints/nosql",
            json={"name": "prod-mongo", "credential_ref": "vault/mongo/prod"},
            headers={"Authorization": "Bearer test-token"},
        )

    assert response.status_code == 201
    assert response.json()["type"] == "nosql"
