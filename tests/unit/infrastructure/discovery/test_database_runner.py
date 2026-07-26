# tests/unit/infrastructure/discovery/test_database_runner.py
from __future__ import annotations

import tempfile

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.domain.endpoints.endpoint import DatabaseEndpoint
from app.domain.objects.data_object import DataObject
from app.domain.objects.object_type import ObjectType
from app.domain.shared.value_objects import CredentialReference
from app.infrastructure.adapters.secrets.noop_secret_manager_adapter import NoopSecretManagerAdapter
from app.infrastructure.discovery.database_runner import DatabaseRunner

_temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_temp_db.close()

_CRED_REF = "vault/db/test"
_SQLITE_PAYLOAD = {"driver": "sqlite+aiosqlite", "database": _temp_db.name.replace("\\", "/")}


@pytest.fixture(autouse=True)
async def seed_db():
    engine = create_async_engine(f"sqlite+aiosqlite:///{_SQLITE_PAYLOAD['database']}", echo=False)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS customers (
                id INTEGER PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                email TEXT,
                balance REAL,
                is_active INTEGER,
                created_at TEXT
            )
        """
            )
        )
    yield
    await engine.dispose()


def _endpoint() -> DatabaseEndpoint:
    return DatabaseEndpoint(
        id="ep-1",
        name="db-prod",
        credential_ref=CredentialReference(_CRED_REF),
    )


def _object(name: str, obj_id: str | None = None) -> DataObject:
    return DataObject(
        id=obj_id or f"obj-{name}",
        asset_id="asset-1",
        name=name,
        type=ObjectType.TABLE,
    )


def _runner() -> DatabaseRunner:
    return DatabaseRunner(
        secret_manager=NoopSecretManagerAdapter(store={_CRED_REF: _SQLITE_PAYLOAD})
    )


@pytest.mark.asyncio
async def test_runner_returns_one_snapshot_per_object() -> None:
    snapshots = await _runner().run(
        asset_id="asset-1",
        scope_include=["customers", "orders"],
        scope_exclude=[],
        endpoint=_endpoint(),
    )
    # Since "orders" doesn't exist, it won't be returned by inspector.get_table_names()
    assert len(snapshots) == 1
    assert snapshots[0].object_name == "customers"


@pytest.mark.asyncio
async def test_runner_captures_columns() -> None:
    snapshots = await _runner().run(
        asset_id="asset-1",
        scope_include=["customers"],
        scope_exclude=[],
        endpoint=_endpoint(),
    )
    field_names = {f.name for f in snapshots[0].fields}
    # customers table has: id, name, email, balance, is_active, created_at
    assert {"id", "name", "email"}.issubset(field_names)


@pytest.mark.asyncio
async def test_runner_marks_primary_key() -> None:
    snapshots = await _runner().run(
        asset_id="asset-1",
        scope_include=["customers"],
        scope_exclude=[],
        endpoint=_endpoint(),
    )
    id_field = next(f for f in snapshots[0].fields if f.name == "id")
    assert id_field.is_primary_key is True
    name_field = next(f for f in snapshots[0].fields if f.name == "name")
    assert name_field.is_primary_key is False


@pytest.mark.asyncio
async def test_runner_sets_runner_type_and_object_name() -> None:
    snapshots = await _runner().run(
        asset_id="asset-1",
        scope_include=["customers"],
        scope_exclude=[],
        endpoint=_endpoint(),
    )
    assert snapshots[0].runner_type == "database"
    assert snapshots[0].object_name == "customers"


@pytest.mark.asyncio
async def test_runner_skips_missing_table_gracefully() -> None:
    snapshots = await _runner().run(
        asset_id="asset-1",
        scope_include=["nonexistent_table"],
        scope_exclude=[],
        endpoint=_endpoint(),
    )
    assert len(snapshots) == 0


@pytest.mark.asyncio
async def test_runner_normalizes_integer_type() -> None:
    snapshots = await _runner().run(
        asset_id="asset-1",
        scope_include=["customers"],
        scope_exclude=[],
        endpoint=_endpoint(),
    )
    id_field = next(f for f in snapshots[0].fields if f.name == "id")
    assert id_field.normalized_type == "integer"
