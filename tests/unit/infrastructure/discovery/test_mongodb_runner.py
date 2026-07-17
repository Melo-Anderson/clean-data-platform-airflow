from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.domain.endpoints.endpoint import NoSqlEndpoint
from app.domain.shared.value_objects import CredentialReference
from app.infrastructure.adapters.secrets.noop_secret_manager_adapter import NoopSecretManagerAdapter
from app.infrastructure.discovery.mongodb_runner import MongoDbRunner

_CRED_REF = "vault/mongo/prod"
_MONGO_URI = "mongodb://localhost:27017/testdb"


def _endpoint() -> NoSqlEndpoint:
    return NoSqlEndpoint(
        id="ep-mongo-1",
        name="prod-mongo",
        credential_ref=CredentialReference(_CRED_REF),
    )


def _runner() -> MongoDbRunner:
    return MongoDbRunner(
        secret_manager=NoopSecretManagerAdapter(store={_CRED_REF: {"uri": _MONGO_URI}})
    )


async def _async_gen(items):
    for item in items:
        yield item


@pytest.mark.asyncio
async def test_runner_uses_json_schema_validator_when_present() -> None:
    """When listCollections returns a $jsonSchema validator, use it."""
    list_collections_result = [
        {
            "name": "users",
            "options": {
                "validator": {
                    "$jsonSchema": {
                        "properties": {
                            "name": {"bsonType": "string"},
                            "age": {"bsonType": "int"},
                        }
                    }
                }
            },
        }
    ]

    with patch("app.infrastructure.discovery.mongodb_runner.AsyncIOMotorClient") as MockClient:
        mock_db = AsyncMock()
        mock_db.list_collections = MagicMock(return_value=_async_gen(list_collections_result))
        mock_collection = AsyncMock()
        mock_collection.estimated_document_count = AsyncMock(return_value=1000)
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_db.users = mock_collection  # for getattr fallback
        MockClient.return_value.__getitem__ = MagicMock(return_value=mock_db)

        runner = _runner()
        snapshots = await runner.run(
            asset_id="asset-1",
            scope_include=["users"],
            scope_exclude=[],
            endpoint=_endpoint(),
        )

    assert len(snapshots) == 1
    assert snapshots[0].object_name == "users"
    assert snapshots[0].runner_type == "mongodb"
    field_names = {f.name for f in snapshots[0].fields}
    assert {"name", "age"}.issubset(field_names)
    name_field = next(f for f in snapshots[0].fields if f.name == "name")
    assert name_field.normalized_type == "string"
    age_field = next(f for f in snapshots[0].fields if f.name == "age")
    assert age_field.normalized_type == "integer"


@pytest.mark.asyncio
async def test_runner_falls_back_to_sampling_when_no_validator() -> None:
    """When no validator is present, infer schema from $sample documents."""
    list_collections_result = [{"name": "orders", "options": {}}]
    sample_docs = [
        {"order_id": "abc", "total": 99.99, "items": [{"sku": "X"}]},
        {"order_id": "def", "total": 10.0, "shipped": True},
    ]

    with patch("app.infrastructure.discovery.mongodb_runner.AsyncIOMotorClient") as MockClient:
        mock_db = AsyncMock()
        mock_db.list_collections = MagicMock(return_value=_async_gen(list_collections_result))
        mock_collection = AsyncMock()
        mock_collection.estimated_document_count = AsyncMock(return_value=500)
        mock_collection.aggregate = MagicMock(return_value=_async_gen(sample_docs))
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_db.orders = mock_collection
        MockClient.return_value.__getitem__ = MagicMock(return_value=mock_db)

        runner = _runner()
        snapshots = await runner.run(
            asset_id="asset-1",
            scope_include=["orders"],
            scope_exclude=[],
            endpoint=_endpoint(),
        )

    assert len(snapshots) == 1
    field_names = {f.name for f in snapshots[0].fields}
    # order_id, total, items, shipped must be discovered
    assert {"order_id", "total", "items", "shipped"}.issubset(field_names)
    # items is a list — must be normalized as "json"
    items_field = next(f for f in snapshots[0].fields if f.name == "items")
    assert items_field.normalized_type == "json"


@pytest.mark.asyncio
async def test_runner_applies_scope_exclude() -> None:
    """Collections matching scope_exclude patterns are skipped."""
    list_collections_result = [
        {"name": "users", "options": {}},
        {"name": "temp_cache", "options": {}},
    ]

    with patch("app.infrastructure.discovery.mongodb_runner.AsyncIOMotorClient") as MockClient:
        mock_db = AsyncMock()
        mock_db.list_collections = MagicMock(return_value=_async_gen(list_collections_result))
        mock_collection = AsyncMock()
        mock_collection.estimated_document_count = AsyncMock(return_value=0)
        mock_collection.aggregate = MagicMock(return_value=_async_gen([]))
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_db.users = mock_collection
        mock_db.temp_cache = mock_collection
        MockClient.return_value.__getitem__ = MagicMock(return_value=mock_db)

        runner = _runner()
        snapshots = await runner.run(
            asset_id="asset-1",
            scope_include=["*"],
            endpoint=_endpoint(),
            scope_exclude=["temp_*"],
        )

    object_names = {s.object_name for s in snapshots}
    assert "users" in object_names
    assert "temp_cache" not in object_names
