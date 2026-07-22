# tests/unit/infrastructure/discovery/test_rest_api_runner.py
from __future__ import annotations

import httpx
import pytest
import respx

from app.domain.endpoints.endpoint import RestApiEndpoint
from app.domain.shared.value_objects import CredentialReference
from app.infrastructure.adapters.secrets.noop_secret_manager_adapter import (
    NoopSecretManagerAdapter,
)
from app.infrastructure.discovery.rest_api_runner import RestApiRunner

_CRED_REF = "vault/api/mock-store"


def _endpoint(auth_type: str = "bearer") -> RestApiEndpoint:
    return RestApiEndpoint(
        id="ep-api-1",
        name="mock-store",
        credential_ref=CredentialReference(path=_CRED_REF),
        base_url="http://test-api",
        auth_type=auth_type,
    )


@pytest.mark.asyncio
async def test_build_client_bearer_sets_authorization_header() -> None:
    secret_manager = NoopSecretManagerAdapter(store={_CRED_REF: {"token": "my-secret-token"}})
    runner = RestApiRunner(secret_manager=secret_manager)

    client = await runner._build_client(_endpoint("bearer"))

    assert str(client.base_url) == "http://test-api"
    assert client.headers["Authorization"] == "Bearer my-secret-token"


@pytest.mark.asyncio
async def test_build_client_api_key_sets_x_api_key_header() -> None:
    secret_manager = NoopSecretManagerAdapter(store={_CRED_REF: {"token": "apikey-xyz"}})
    runner = RestApiRunner(secret_manager=secret_manager)

    client = await runner._build_client(_endpoint("api_key"))

    assert client.headers["x-api-key"] == "apikey-xyz"


@pytest.mark.asyncio
async def test_build_client_raises_for_unsupported_auth_type() -> None:
    secret_manager = NoopSecretManagerAdapter(store={_CRED_REF: {"token": "some-token"}})
    runner = RestApiRunner(secret_manager=secret_manager)

    with pytest.raises(ValueError, match="Unsupported auth_type"):
        await runner._build_client(_endpoint("oauth2"))


@pytest.mark.asyncio
async def test_infer_fields_from_flat_sample() -> None:
    secret_manager = NoopSecretManagerAdapter(store={_CRED_REF: {"token": "t"}})
    runner = RestApiRunner(secret_manager=secret_manager)

    sample = {"id": 1, "name": "Alice", "score": 9.5, "active": True, "tags": ["a"]}
    fields = runner._infer_fields_from_sample("users", sample)

    by_name = {f.name: f for f in fields}
    assert by_name["id"].normalized_type == "integer"
    assert by_name["id"].is_primary_key is True
    assert by_name["name"].normalized_type == "string"
    assert by_name["score"].normalized_type == "float"
    assert by_name["active"].normalized_type == "boolean"
    assert by_name["tags"].normalized_type == "json"
    # source_type must include the Python type name
    assert "int" in by_name["id"].source_type


@pytest.mark.asyncio
async def test_infer_fields_detects_fk_suffix() -> None:
    secret_manager = NoopSecretManagerAdapter(store={_CRED_REF: {"token": "t"}})
    runner = RestApiRunner(secret_manager=secret_manager)

    sample = {"order_id": 42, "customer_id": 7, "total": 199.99}
    fields = runner._infer_fields_from_sample("orders", sample)

    by_name = {f.name: f for f in fields}
    # order_id and customer_id both end in _id — primary key heuristic applies only to exact "id"
    assert by_name["order_id"].is_primary_key is False  # not bare "id"
    assert by_name["customer_id"].is_primary_key is False


@pytest.mark.asyncio
@respx.mock
async def test_snapshot_from_sample_unwraps_envelope() -> None:
    """Payload with 'data' wrapper must be unwrapped before sampling."""
    secret_manager = NoopSecretManagerAdapter(store={_CRED_REF: {"token": "t"}})
    runner = RestApiRunner(secret_manager=secret_manager)
    endpoint = _endpoint("bearer")

    respx.get("http://test-api/api/v1/products").mock(
        return_value=httpx.Response(
            200,
            json={"data": [{"id": 1, "name": "Widget", "price": 4.99}], "pagination": {}},
        )
    )

    async with await runner._build_client(endpoint) as client:
        snapshot = await runner._snapshot_from_sample(
            endpoint, asset_id="asset-1", resource_name="products", client=client
        )

    assert snapshot.runner_type == "rest_api"
    assert "products" in snapshot.object_name
    field_names = {f.name for f in snapshot.fields}
    assert {"id", "name", "price"}.issubset(field_names)
    id_field = next(f for f in snapshot.fields if f.name == "id")
    assert id_field.is_primary_key is True


_OPENAPI_SPEC = {
    "openapi": "3.0.0",
    "components": {
        "schemas": {
            "Product": {
                "type": "object",
                "required": ["id", "name"],
                "properties": {
                    "id": {"type": "integer", "description": "Primary key"},
                    "name": {"type": "string"},
                    "price": {"type": "number", "format": "double"},
                    "created_at": {"type": "string", "format": "date-time"},
                    "sku": {"type": "string", "format": "uuid"},
                    "tags": {"type": "array"},
                    "x-pk": {"type": "integer", "x-primary-key": True},
                },
            },
            "Customer": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer", "format": "int64"},
                    "full_name": {"type": "string"},
                },
            },
        }
    },
}


@pytest.mark.asyncio
@respx.mock
async def test_try_openapi_discovery_returns_snapshots_for_matched_schemas() -> None:
    secret_manager = NoopSecretManagerAdapter(store={_CRED_REF: {"token": "t"}})
    runner = RestApiRunner(secret_manager=secret_manager)
    endpoint = _endpoint("bearer")

    respx.get("http://test-api/openapi.json").mock(
        return_value=httpx.Response(200, json=_OPENAPI_SPEC)
    )

    async with await runner._build_client(endpoint) as client:
        snapshots = await runner._try_openapi_discovery(
            endpoint,
            asset_id="asset-1",
            scope_include=["Product", "Customer"],
            client=client,
        )

    assert snapshots is not None
    assert len(snapshots) == 2
    names = {s.object_name for s in snapshots}
    assert any("Product" in n for n in names)
    assert any("Customer" in n for n in names)


@pytest.mark.asyncio
@respx.mock
async def test_openapi_discovery_maps_types_correctly() -> None:
    secret_manager = NoopSecretManagerAdapter(store={_CRED_REF: {"token": "t"}})
    runner = RestApiRunner(secret_manager=secret_manager)
    endpoint = _endpoint("bearer")

    respx.get("http://test-api/openapi.json").mock(
        return_value=httpx.Response(200, json=_OPENAPI_SPEC)
    )

    async with await runner._build_client(endpoint) as client:
        snapshots = await runner._try_openapi_discovery(
            endpoint, asset_id="a", scope_include=["Product"], client=client
        )

    assert snapshots is not None
    snapshot = snapshots[0]
    by_name = {f.name: f for f in snapshot.fields}

    assert by_name["id"].normalized_type == "integer"
    assert by_name["id"].is_primary_key is True
    assert by_name["id"].nullable is False  # "id" in required list
    assert by_name["price"].normalized_type == "decimal"
    assert by_name["created_at"].normalized_type == "timestamp"
    assert by_name["tags"].normalized_type == "json"
    assert by_name["x-pk"].is_primary_key is True  # x-primary-key extension


@pytest.mark.asyncio
@respx.mock
async def test_try_openapi_discovery_returns_none_on_404() -> None:
    secret_manager = NoopSecretManagerAdapter(store={_CRED_REF: {"token": "t"}})
    runner = RestApiRunner(secret_manager=secret_manager)
    endpoint = _endpoint()

    respx.get("http://test-api/openapi.json").mock(return_value=httpx.Response(404))

    async with await runner._build_client(endpoint) as client:
        result = await runner._try_openapi_discovery(
            endpoint, asset_id="a", scope_include=["Product"], client=client
        )

    assert result is None


@pytest.mark.asyncio
@respx.mock
async def test_run_uses_openapi_when_available() -> None:
    """run() should prefer OpenAPI if it returns 200 with matching schemas."""
    secret_manager = NoopSecretManagerAdapter(store={_CRED_REF: {"token": "t"}})
    runner = RestApiRunner(secret_manager=secret_manager)
    endpoint = _endpoint("bearer")

    respx.get("http://test-api/openapi.json").mock(
        return_value=httpx.Response(200, json=_OPENAPI_SPEC)
    )

    snapshots = await runner.run(
        asset_id="asset-1",
        scope_include=["Product"],
        scope_exclude=[],
        endpoint=endpoint,
    )

    assert len(snapshots) == 1
    assert snapshots[0].extra["discovery_method"] == "openapi"
    id_field = next(f for f in snapshots[0].fields if f.name == "id")
    assert id_field.is_primary_key is True


@pytest.mark.asyncio
@respx.mock
async def test_run_falls_back_to_sampling_when_no_openapi() -> None:
    """run() should use payload sampling if OpenAPI returns 404."""
    secret_manager = NoopSecretManagerAdapter(store={_CRED_REF: {"token": "t"}})
    runner = RestApiRunner(secret_manager=secret_manager)
    endpoint = _endpoint("bearer")

    respx.get("http://test-api/openapi.json").mock(return_value=httpx.Response(404))
    respx.get("http://test-api/api/v1/products").mock(
        return_value=httpx.Response(
            200,
            json={"data": [{"id": 1, "name": "Widget", "price": 9.99}]},
        )
    )

    snapshots = await runner.run(
        asset_id="asset-1",
        scope_include=["products"],
        scope_exclude=[],
        endpoint=endpoint,
    )

    assert len(snapshots) == 1
    assert snapshots[0].extra["discovery_method"] == "payload_sampling"
    field_names = {f.name for f in snapshots[0].fields}
    assert {"id", "name", "price"}.issubset(field_names)


@pytest.mark.asyncio
async def test_run_raises_type_error_for_non_rest_endpoint() -> None:
    from app.domain.endpoints.endpoint import NoSqlEndpoint
    from app.domain.shared.value_objects import CredentialReference

    secret_manager = NoopSecretManagerAdapter(store={_CRED_REF: {"token": "t"}})
    runner = RestApiRunner(secret_manager=secret_manager)
    wrong_endpoint = NoSqlEndpoint(
        id="ep-mongo",
        name="mongo",
        credential_ref=CredentialReference(path=_CRED_REF),
    )

    with pytest.raises(TypeError, match="RestApiRunner only supports RestApiEndpoint"):
        await runner.run(
            asset_id="a",
            scope_include=["col"],
            scope_exclude=[],
            endpoint=wrong_endpoint,
        )
