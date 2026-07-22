# tests/e2e/test_rest_api_discovery_e2e.py
from __future__ import annotations

import os

import pytest

from app.domain.endpoints.endpoint import RestApiEndpoint
from app.domain.shared.value_objects import CredentialReference
from app.infrastructure.adapters.secrets.noop_secret_manager_adapter import (
    NoopSecretManagerAdapter,
)
from app.infrastructure.discovery.rest_api_runner import RestApiRunner

pytestmark = pytest.mark.e2e

_CRED_REF = "vault/api/mock-store"
_MOCK_API_TOKEN = "e2e-test-token"  # mock_store_api does not validate tokens


def _base_url() -> str:
    host = os.getenv("MOCK_API_HOST", "localhost")
    return f"http://{host}:8081"


def _endpoint() -> RestApiEndpoint:
    return RestApiEndpoint(
        id="ep-mock-store",
        name="mock-store-api",
        credential_ref=CredentialReference(path=_CRED_REF),
        base_url=_base_url(),
        auth_type="bearer",
    )


def _runner() -> RestApiRunner:
    return RestApiRunner(
        secret_manager=NoopSecretManagerAdapter(store={_CRED_REF: {"token": _MOCK_API_TOKEN}})
    )


@pytest.mark.asyncio
async def test_rest_api_discovery_openapi_finds_product_schema() -> None:
    """OpenAPI spec from mock_store_api must expose Product with known fields."""
    runner = _runner()

    snapshots = await runner.run(
        asset_id="asset-e2e-1",
        scope_include=["Product"],
        scope_exclude=[],
        endpoint=_endpoint(),
    )

    assert len(snapshots) >= 1, "Expected at least one snapshot for 'Product'"
    snapshot = snapshots[0]
    assert snapshot.runner_type == "rest_api"
    assert snapshot.extra["discovery_method"] == "openapi"

    field_names = {f.name for f in snapshot.fields}
    assert "id" in field_names
    assert "name" in field_names
    assert "price" in field_names

    id_field = next(f for f in snapshot.fields if f.name == "id")
    assert id_field.is_primary_key is True


@pytest.mark.asyncio
async def test_rest_api_discovery_openapi_finds_multiple_schemas() -> None:
    """Glob pattern '*' should discover all schemas exposed by the API."""
    runner = _runner()

    snapshots = await runner.run(
        asset_id="asset-e2e-2",
        scope_include=["*"],
        scope_exclude=[],
        endpoint=_endpoint(),
    )

    schema_names = [s.extra.get("schema_name", "") for s in snapshots]
    assert len(snapshots) >= 3, f"Expected Product, Customer, Order schemas. Got: {schema_names}"


@pytest.mark.asyncio
async def test_rest_api_discovery_respects_scope_exclude() -> None:
    """scope_exclude must filter out matched object_names."""
    runner = _runner()

    snapshots = await runner.run(
        asset_id="asset-e2e-3",
        scope_include=["*"],
        scope_exclude=["*/Customer"],
        endpoint=_endpoint(),
    )

    schema_names = [s.extra.get("schema_name", "") for s in snapshots]
    assert "Customer" not in schema_names
