from __future__ import annotations

import pytest

from app.domain.pipelines.watermark import WatermarkType
from app.infrastructure.adapters.compute.source_dtos import PipelineExecutionTargetDTO
from app.infrastructure.adapters.compute.source_translators import (
    DatabaseManifestTranslator,
    RestApiManifestTranslator,
    StorageManifestTranslator,
)
from app.infrastructure.adapters.omnibeam.omnibeam_manifest_builder import OmniBeamManifestBuilder
from app.infrastructure.adapters.omnibeam.omnibeam_manifest_schema import OmniBeamWatermarkConfig


def test_dto_from_config_extracts_watermark_from_source_objects() -> None:
    config = {
        "source_objects": [
            {
                "object_name": "users",
                "source_type": "postgres",
                "credential_ref": "vault:db",
                "watermark_column": "updated_at",
                "watermark_type": WatermarkType.TIMESTAMP,
                "watermark_value": "2026-10-24T00:00:00",
            }
        ]
    }
    dto = PipelineExecutionTargetDTO.from_config(config)
    assert dto.watermark_column == "updated_at"
    assert dto.watermark_type == "timestamp"
    assert dto.watermark_value == "2026-10-24T00:00:00"


def test_database_manifest_translator_table_sets_query_filter_and_watermark() -> None:
    builder = OmniBeamManifestBuilder()
    translator = DatabaseManifestTranslator()
    config = {
        "watermark": {
            "column": "id",
            "type": "integer",
            "value": "100",
        }
    }
    creds = {
        "driver": "postgres",
        "host": "localhost",
        "port": 5432,
        "database": "db",
        "user": "usr",
        "password": "pwd",
    }
    source_cfg = translator.translate(
        object_name="orders",
        config=config,
        creds=creds,
        builder=builder,
    )
    assert source_cfg.query_filter == "id > 100"
    assert source_cfg.watermark == OmniBeamWatermarkConfig(column="id", type="integer", value="100")


def test_database_manifest_translator_custom_query_binds_placeholder() -> None:
    builder = OmniBeamManifestBuilder()
    translator = DatabaseManifestTranslator()
    config = {
        "extraction_query": "SELECT * FROM orders WHERE id > :watermark_value",
        "watermark": {
            "column": "id",
            "type": "integer",
            "value": "500",
        },
    }
    creds = {
        "driver": "postgres",
        "host": "localhost",
        "port": 5432,
        "database": "db",
        "user": "usr",
        "password": "pwd",
    }
    source_cfg = translator.translate(
        object_name="orders",
        config=config,
        creds=creds,
        builder=builder,
    )
    assert source_cfg.query == "SELECT * FROM orders WHERE id > 500"
    assert source_cfg.watermark == OmniBeamWatermarkConfig(column="id", type="integer", value="500")


def test_storage_manifest_translator_raises_on_watermark() -> None:
    builder = OmniBeamManifestBuilder()
    translator = StorageManifestTranslator()
    config = {
        "source_type": "storage",
        "paths": ["gs://bucket/data/*.csv"],
        "watermark": {
            "column": "mtime",
            "type": "timestamp",
            "value": "2026-10-24T00:00:00",
        },
    }
    with pytest.raises(ValueError, match="not supported"):
        translator.translate(
            object_name="files",
            config=config,
            creds={},
            builder=builder,
        )


def test_rest_api_manifest_translator_binds_query_params_and_watermark() -> None:
    builder = OmniBeamManifestBuilder()
    translator = RestApiManifestTranslator()
    config = {
        "endpoint": "/v1/events",
        "query_params": {"since": ":watermark_value", "limit": "50"},
        "watermark": {
            "column": "event_timestamp",
            "type": "timestamp",
            "value": "2026-10-24T10:00:00",
        },
    }
    creds = {"base_url": "https://api.example.com"}
    source_cfg = translator.translate(
        object_name="events",
        config=config,
        creds=creds,
        builder=builder,
    )
    assert source_cfg.query_params["since"] == "2026-10-24T10:00:00"
    assert source_cfg.query_params["limit"] == "50"
    assert source_cfg.watermark == OmniBeamWatermarkConfig(
        column="event_timestamp", type="timestamp", value="2026-10-24T10:00:00"
    )
