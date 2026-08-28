from __future__ import annotations

import json
from datetime import UTC, datetime

from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.pipelines.pipeline_run_file import PipelineRunFile
from app.infrastructure.adapters.omnibeam.omnibeam_manifest_builder import (
    OmniBeamManifestBuilder,
)


def test_build_omnibeam_manifest_csv() -> None:
    now = datetime.now(tz=UTC)
    files = [
        PipelineRunFile(
            id="f-1",
            pipeline_run_id="run-001",
            file_path="gs://landing/orders_1.csv",
            file_name="orders_1.csv",
            file_size_bytes=100,
            mtime=now,
            hash_md5="abc",
        ),
        PipelineRunFile(
            id="f-2",
            pipeline_run_id="run-001",
            file_path="gs://landing/orders_2.csv",
            file_name="orders_2.csv",
            file_size_bytes=200,
            mtime=now,
            hash_md5="def",
        ),
    ]

    snapshot = SchemaSnapshot(
        object_id="asset.orders",
        object_name="orders",
        fields=[
            SchemaField(name="id", source_type="BIGINT", normalized_type="bigint", nullable=False),
            SchemaField(
                name="customer_id", source_type="VARCHAR", normalized_type="string", nullable=False
            ),
            SchemaField(
                name="amount",
                source_type="DECIMAL(10,2)",
                normalized_type="decimal",
                nullable=False,
            ),
        ],
    )

    builder = OmniBeamManifestBuilder()
    manifest = builder.build(
        pipeline_id="pipe-orders",
        run_id="run-001",
        runner="dataflow",
        files=files,
        snapshot=snapshot,
        output_path="gs://lakehouse/bronze/orders/dt=2026-08-27",
        quarantine_path="gs://lakehouse/dlq/orders/dt=2026-08-27",
        sensitive_fields=["customer_id"],
        quality_rules=[{"type": "not_null", "column": "id"}],
    )

    payload = json.loads(manifest.to_json())
    assert payload["pipeline_id"] == "pipe-orders"
    assert payload["run_id"] == "run-001"
    assert payload["runner"] == "dataflow"
    assert payload["source"]["type"] == "storage"
    assert payload["source"]["paths"] == ["gs://landing/orders_1.csv", "gs://landing/orders_2.csv"]
    assert payload["source"]["format"] == "csv"
    assert len(payload["source"]["schema"]["fields"]) == 3
    assert payload["destination"]["output_path"] == "gs://lakehouse/bronze/orders/dt=2026-08-27"
    assert payload["destination"]["output_format"] == "parquet"
    assert payload["dlq_config"]["enabled"] is True
    assert payload["security"]["sensitive_fields"] == ["customer_id"]


def test_build_omnibeam_manifest_jsonl() -> None:
    now = datetime.now(tz=UTC)
    files = [
        PipelineRunFile(
            id="f-1",
            pipeline_run_id="run-002",
            file_path="/data/events.json",
            file_name="events.json",
            file_size_bytes=500,
            mtime=now,
            hash_md5="abcjson",
        )
    ]
    snapshot = SchemaSnapshot(
        object_id="asset.events",
        object_name="events",
        fields=[
            SchemaField(
                name="event_id", source_type="VARCHAR", normalized_type="string", nullable=False
            ),
        ],
    )

    builder = OmniBeamManifestBuilder()
    manifest = builder.build(
        pipeline_id="pipe-events",
        run_id="run-002",
        runner="direct",
        files=files,
        snapshot=snapshot,
        output_path="/data/output/events",
        quarantine_path="/data/output/dlq",
    )

    payload = json.loads(manifest.to_json())
    assert payload["source"]["format"] == "jsonl"
    assert payload["runner"] == "direct"


def test_build_omnibeam_manifest_database() -> None:
    snapshot = SchemaSnapshot(
        object_id="asset.postgres_users",
        object_name="users",
        fields=[
            SchemaField(
                name="id", source_type="INTEGER", normalized_type="integer", nullable=False
            ),
            SchemaField(name="email", source_type="TEXT", normalized_type="string", nullable=True),
        ],
    )
    builder = OmniBeamManifestBuilder()
    db_source = builder.build_database_source(
        credential_ref="secret/pg",
        snapshot=snapshot,
        table="users",
        partition_column="id",
        num_partitions=4,
    )
    manifest = builder.build(
        pipeline_id="pipe-db",
        run_id="run-003",
        source_config=db_source,
        output_path="/out/users",
        quarantine_path="/out/dlq",
    )
    payload = json.loads(manifest.to_json())
    assert payload["source"]["type"] == "database"
    assert payload["source"]["credential_ref"] == "secret/pg"
    assert payload["source"]["table"] == "users"
    assert len(payload["source"]["schema"]["fields"]) == 2


def test_build_omnibeam_manifest_rest_api() -> None:
    snapshot = SchemaSnapshot(
        object_id="asset.api_store",
        object_name="orders",
        fields=[
            SchemaField(
                name="order_id", source_type="STRING", normalized_type="string", nullable=False
            ),
            SchemaField(
                name="amount", source_type="FLOAT", normalized_type="float", nullable=False
            ),
        ],
    )
    builder = OmniBeamManifestBuilder()
    api_source = builder.build_rest_api_source(
        base_url="https://api.store.local",
        path="/v1/orders",
        snapshot=snapshot,
        auth_type="bearer",
    )
    manifest = builder.build(
        pipeline_id="pipe-api",
        run_id="run-004",
        source_config=api_source,
        output_path="/out/api",
        quarantine_path="/out/dlq",
    )
    payload = json.loads(manifest.to_json())
    assert payload["source"]["type"] == "rest_api"
    assert payload["source"]["base_url"] == "https://api.store.local"
    assert payload["source"]["path"] == "/v1/orders"


def test_build_omnibeam_manifest_mongodb() -> None:
    snapshot = SchemaSnapshot(
        object_id="asset.mongo_events",
        object_name="clickstream",
        fields=[
            SchemaField(
                name="_id", source_type="OBJECT_ID", normalized_type="string", nullable=False
            ),
            SchemaField(
                name="event_type", source_type="STRING", normalized_type="string", nullable=False
            ),
        ],
    )
    builder = OmniBeamManifestBuilder()
    mongo_source = builder.build_mongo_source(
        credential_ref="secret/mongo",
        database="analytics",
        collection="events",
        snapshot=snapshot,
    )
    manifest = builder.build(
        pipeline_id="pipe-mongo",
        run_id="run-005",
        source_config=mongo_source,
        output_path="/out/mongo",
        quarantine_path="/out/dlq",
    )
    payload = json.loads(manifest.to_json())
    assert payload["source"]["type"] == "mongodb"
    assert payload["source"]["database"] == "analytics"
    assert payload["source"]["collection"] == "events"
