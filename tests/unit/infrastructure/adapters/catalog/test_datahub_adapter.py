# tests/unit/infrastructure/adapters/catalog/test_datahub_adapter.py
from __future__ import annotations

import sys
import uuid
from unittest.mock import MagicMock, patch

import pytest

from app.application.shared.adapters.catalog_adapter import CatalogPublishError
from app.domain.assets.data_asset import DataAsset
from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.lineage.lineage_mapping import LineageMapping
from app.infrastructure.adapters.catalog.datahub_adapter import DataHubCatalogAdapter


@pytest.fixture
def mock_datahub():
    mock_dh = MagicMock()
    mock_emitter = MagicMock()
    mock_dh.emitter.rest_emitter.DatahubRestEmitter = mock_emitter
    mock_dh.metadata.schema_classes.NumberTypeClass = MagicMock(return_value="NumberTypeClass()")
    mock_dh.metadata.schema_classes.BooleanTypeClass = MagicMock(return_value="BooleanTypeClass()")
    mock_dh.metadata.schema_classes.StringTypeClass = MagicMock(return_value="StringTypeClass()")
    mock_dh.metadata.schema_classes.SchemaFieldClass = MagicMock
    mock_dh.metadata.schema_classes.SchemaMetadataClass = MagicMock
    mock_dh.metadata.schema_classes.MetadataChangeProposalWrapper = MagicMock
    mock_dh.metadata.schema_classes.UpstreamLineageClass = MagicMock
    mock_dh.metadata.schema_classes.UpstreamClass = MagicMock

    with patch.dict(
        sys.modules,
        {
            "datahub": mock_dh,
            "datahub.emitter": mock_dh.emitter,
            "datahub.emitter.mcp": mock_dh.emitter.mcp,
            "datahub.emitter.rest_emitter": mock_dh.emitter.rest_emitter,
            "datahub.metadata": mock_dh.metadata,
            "datahub.metadata.schema_classes": mock_dh.metadata.schema_classes,
        },
    ):
        yield mock_dh


def test_map_field_type_integer_returns_number_type(mock_datahub):
    adapter = DataHubCatalogAdapter("http://test")
    result = adapter._map_field_type("integer")
    assert result == "NumberTypeClass()"
    mock_datahub.metadata.schema_classes.NumberTypeClass.assert_called_once()


def test_map_field_type_boolean(mock_datahub):
    adapter = DataHubCatalogAdapter("http://test")
    result = adapter._map_field_type("boolean")
    assert result == "BooleanTypeClass()"
    mock_datahub.metadata.schema_classes.BooleanTypeClass.assert_called_once()


def test_map_field_type_fallback_returns_string(mock_datahub):
    adapter = DataHubCatalogAdapter("http://test")
    result = adapter._map_field_type("unknown_type")
    assert result == "StringTypeClass()"
    mock_datahub.metadata.schema_classes.StringTypeClass.assert_called_once()


@pytest.mark.asyncio
async def test_publish_schema_calls_emitter(mock_datahub):
    adapter = DataHubCatalogAdapter("http://test")
    asset = MagicMock(spec=DataAsset)
    snapshot = SchemaSnapshot(
        object_id="obj_1",
        object_name="table_1",
        runner_type="postgres",
        fields=[
            SchemaField(
                name="f1", source_type="VARCHAR", normalized_type="string", description="foo"
            )
        ],
    )

    mock_emitter_instance = mock_datahub.emitter.rest_emitter.DatahubRestEmitter.return_value

    await adapter.publish_schema(asset, snapshot)

    mock_emitter_instance.emit.assert_called_once()
    mock_datahub.emitter.mcp.MetadataChangeProposalWrapper.assert_called_once()


@pytest.mark.asyncio
async def test_publish_schema_raises_catalog_publish_error_on_exception(mock_datahub):
    adapter = DataHubCatalogAdapter("http://test")
    asset = MagicMock(spec=DataAsset)
    asset.id = "a1"
    asset.name = "test_asset"
    snapshot = SchemaSnapshot(
        object_id="obj_1", object_name="table_1", runner_type="postgres", fields=[]
    )

    mock_emitter_instance = mock_datahub.emitter.rest_emitter.DatahubRestEmitter.return_value
    mock_emitter_instance.emit.side_effect = Exception("Network error")

    with pytest.raises(CatalogPublishError, match="DataHub schema publish failed"):
        await adapter.publish_schema(asset, snapshot)


@pytest.mark.asyncio
async def test_publish_lineage_calls_emitter_with_mcp(mock_datahub):
    adapter = DataHubCatalogAdapter("http://test")
    mock_emitter_instance = mock_datahub.emitter.rest_emitter.DatahubRestEmitter.return_value

    mapping = LineageMapping(
        id=str(uuid.uuid4()),
        pipeline_id="p1",
        source_object_id="src1",
        destination_object_id="dest1",
        column_mappings=[],
    )

    await adapter.publish_lineage(mapping)

    mock_emitter_instance.emit.assert_called_once()
    mock_datahub.emitter.mcp.MetadataChangeProposalWrapper.assert_called_once()


@pytest.mark.asyncio
async def test_publish_lineage_raises_on_failure(mock_datahub):
    adapter = DataHubCatalogAdapter("http://test")
    mock_emitter_instance = mock_datahub.emitter.rest_emitter.DatahubRestEmitter.return_value
    mock_emitter_instance.emit.side_effect = Exception("Failed")

    mapping = LineageMapping(
        id=str(uuid.uuid4()),
        pipeline_id="p1",
        source_object_id="src1",
        destination_object_id="dest1",
        column_mappings=[],
    )

    with pytest.raises(CatalogPublishError, match="DataHub lineage publish failed"):
        await adapter.publish_lineage(mapping)


def test_get_emitter_raises_when_sdk_not_installed():
    with patch.dict(sys.modules, {"datahub.emitter.rest_emitter": None}):
        adapter = DataHubCatalogAdapter("http://test")
        with pytest.raises(CatalogPublishError, match="datahub library is not installed"):
            adapter._get_emitter()
