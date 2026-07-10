from __future__ import annotations

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.domain.assets.data_asset import DataAsset
from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.lineage.lineage_mapping import ElementLineage, LineageMapping
from app.infrastructure.adapters.catalog.database_catalog_adapter import DatabaseCatalogAdapter
from app.infrastructure.persistence.base_model import Base
from app.infrastructure.persistence.models.catalog_schema_version_model import (
    CatalogSchemaVersionModel,
)
from app.infrastructure.persistence.models.data_asset_model import DataAssetModel
from app.infrastructure.persistence.models.data_object_model import DataObjectModel
from app.infrastructure.persistence.models.lineage_mapping_model import LineageMappingModel
from app.infrastructure.persistence.models.pipeline_model import PipelineModel


@pytest.fixture
async def session_factory():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with factory() as session:
        session.add_all(
            [
                DataAssetModel(
                    id="asset-1",
                    name="sales",
                    description="d",
                    owner_email="x@y.com",
                    discovery_schedule="* * * * *",
                ),
                DataObjectModel(id="obj-1", asset_id="asset-1", name="orders", type="table"),
                DataObjectModel(id="obj-2", asset_id="asset-1", name="customers", type="table"),
                PipelineModel(
                    id="pipe-1",
                    name="etl",
                    type="ingestion",
                    owner_email="x@y.com",
                    schema_version="v1",
                    schedule={},
                    transform={},
                    compute={},
                    airflow={},
                    discovery_task={},
                ),
            ]
        )
        await session.commit()
    yield factory
    await engine.dispose()


def _make_asset() -> DataAsset:
    return DataAsset(
        id="asset-1",
        name="sales",
        description="d",
        owner="x@y.com",
        tags=[],
        policy_tags=[],
        discovery_schedule="* * * * *",
    )


def _make_snapshot(fields: list[SchemaField]) -> SchemaSnapshot:
    return SchemaSnapshot(
        object_id="obj-1", object_name="orders", runner_type="sqlite", fields=fields
    )


# ---------- publish_schema ----------


@pytest.mark.asyncio
async def test_publish_schema_creates_first_version(session_factory: async_sessionmaker) -> None:
    adapter = DatabaseCatalogAdapter(session_factory)
    snap = _make_snapshot(
        [SchemaField(name="id", source_type="INT", normalized_type="integer", nullable=False)]
    )

    await adapter.publish_schema(_make_asset(), snap)

    async with session_factory() as s:
        rows = (
            (await s.execute(select(CatalogSchemaVersionModel).filter_by(object_id="obj-1")))
            .scalars()
            .all()
        )
    assert len(rows) == 1
    assert rows[0].version == 1
    assert rows[0].snapshot_json[0]["name"] == "id"


@pytest.mark.asyncio
async def test_publish_schema_is_idempotent_when_unchanged(
    session_factory: async_sessionmaker,
) -> None:
    adapter = DatabaseCatalogAdapter(session_factory)
    field = SchemaField(name="id", source_type="INT", normalized_type="integer", nullable=False)
    snap = _make_snapshot([field])

    await adapter.publish_schema(_make_asset(), snap)
    await adapter.publish_schema(_make_asset(), snap)  # identical — must NOT create v2

    async with session_factory() as s:
        rows = (
            (await s.execute(select(CatalogSchemaVersionModel).filter_by(object_id="obj-1")))
            .scalars()
            .all()
        )
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_publish_schema_increments_version_on_structural_change(
    session_factory: async_sessionmaker,
) -> None:
    adapter = DatabaseCatalogAdapter(session_factory)
    snap_v1 = _make_snapshot(
        [SchemaField(name="id", source_type="INT", normalized_type="integer", nullable=False)]
    )
    snap_v2 = _make_snapshot(
        [
            SchemaField(name="id", source_type="INT", normalized_type="integer", nullable=False),
            SchemaField(
                name="name", source_type="VARCHAR", normalized_type="string", nullable=True
            ),
        ]
    )

    await adapter.publish_schema(_make_asset(), snap_v1)
    await adapter.publish_schema(_make_asset(), snap_v2)

    async with session_factory() as s:
        rows = (
            (
                await s.execute(
                    select(CatalogSchemaVersionModel)
                    .filter_by(object_id="obj-1")
                    .order_by(CatalogSchemaVersionModel.version)
                )
            )
            .scalars()
            .all()
        )
    assert len(rows) == 2
    assert rows[0].version == 1 and len(rows[0].snapshot_json) == 1
    assert rows[1].version == 2 and len(rows[1].snapshot_json) == 2


# ---------- publish_lineage ----------


@pytest.mark.asyncio
async def test_publish_lineage_creates_edge(session_factory: async_sessionmaker) -> None:
    adapter = DatabaseCatalogAdapter(session_factory)
    mapping = LineageMapping(
        id="lin-1",
        pipeline_id="pipe-1",
        source_object_id="obj-1",
        destination_object_id="obj-2",
        column_mappings=[ElementLineage(source_column="id", destination_column="order_id")],
    )

    await adapter.publish_lineage(mapping)

    async with session_factory() as s:
        rows = (
            (await s.execute(select(LineageMappingModel).filter_by(pipeline_id="pipe-1")))
            .scalars()
            .all()
        )
    assert len(rows) == 1
    assert rows[0].column_mappings[0]["source_column"] == "id"


@pytest.mark.asyncio
async def test_publish_lineage_upserts_existing_edge(session_factory: async_sessionmaker) -> None:
    adapter = DatabaseCatalogAdapter(session_factory)
    mapping_v1 = LineageMapping(
        id="lin-1",
        pipeline_id="pipe-1",
        source_object_id="obj-1",
        destination_object_id="obj-2",
        column_mappings=[ElementLineage(source_column="id", destination_column="order_id")],
    )
    mapping_v2 = LineageMapping(
        id="lin-2",
        pipeline_id="pipe-1",  # same edge
        source_object_id="obj-1",
        destination_object_id="obj-2",
        column_mappings=[
            ElementLineage(source_column="id", destination_column="order_id"),
            ElementLineage(source_column="total", destination_column="amount"),
        ],
    )

    await adapter.publish_lineage(mapping_v1)
    await adapter.publish_lineage(mapping_v2)  # must update, not duplicate

    async with session_factory() as s:
        rows = (await s.execute(select(LineageMappingModel))).scalars().all()
    assert len(rows) == 1
    assert len(rows[0].column_mappings) == 2


# ---------- update_policy_tags ----------


@pytest.mark.asyncio
async def test_update_policy_tags_creates_new_version(session_factory: async_sessionmaker) -> None:
    """Policy tag updates must create a new immutable version, not mutate a historical row."""
    adapter = DatabaseCatalogAdapter(session_factory)
    snap = _make_snapshot(
        [SchemaField(name="cpf", source_type="VARCHAR", normalized_type="string", nullable=True)]
    )
    await adapter.publish_schema(_make_asset(), snap)

    await adapter.update_policy_tags("obj-1", {"cpf": "PII"})

    async with session_factory() as s:
        rows = (
            (
                await s.execute(
                    select(CatalogSchemaVersionModel)
                    .filter_by(object_id="obj-1")
                    .order_by(CatalogSchemaVersionModel.version)
                )
            )
            .scalars()
            .all()
        )
    assert len(rows) == 2  # v1 (no tag) + v2 (with tag applied)
    assert rows[0].snapshot_json[0].get("policy_tag") is None  # v1 unchanged
    assert rows[1].snapshot_json[0]["policy_tag"] == "PII"  # v2 with tag
