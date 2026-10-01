from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.pipelines.pipeline_run_file import PipelineRunFile
from app.infrastructure.adapters.omnibeam.omnibeam_manifest_builder import (
    OmniBeamManifestBuilder,
)
from app.infrastructure.adapters.omnibeam.omnibeam_manifest_schema import (
    OmniBeamDlqConfig,
    OmniBeamEncryptionConfig,
    OmniBeamFormatOptions,
    OmniBeamManifest,
    OmniBeamQualityRule,
    OmniBeamSecretsConfig,
    OmniBeamStorageDestinationConfig,
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
    assert payload.get("database_source") is None
    assert payload.get("api_source") is None
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
    assert payload.get("database_source") is None
    assert payload.get("api_source") is None


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
        driver="postgres",
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
    assert payload.get("source") is None
    assert payload.get("api_source") is None
    assert payload["database_source"]["driver"] == "postgres"
    assert payload["database_source"]["table"] == "users"
    assert payload["database_source"]["credential_ref"] == "secret/pg"
    assert payload["database_source"]["partition_config"]["partition_column"] == "id"
    assert payload["database_source"]["partition_config"]["num_partitions"] == 4
    assert len(payload["database_source"]["schema"]["fields"]) == 2


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
        endpoint="/v1/orders",
        snapshot=snapshot,
        auth_type="bearer_token",
        token_ref="secret/token",
        pagination_type="page_number",
        page_size=50,
    )
    manifest = builder.build(
        pipeline_id="pipe-api",
        run_id="run-004",
        source_config=api_source,
        output_path="/out/api",
        quarantine_path="/out/dlq",
    )
    payload = json.loads(manifest.to_json())
    assert payload.get("source") is None
    assert payload.get("database_source") is None
    assert payload["api_source"]["base_url"] == "https://api.store.local"
    assert payload["api_source"]["endpoint"] == "/v1/orders"
    assert payload["api_source"]["auth"]["type"] == "bearer_token"
    assert payload["api_source"]["auth"]["token_ref"] == "secret/token"
    assert payload["api_source"]["pagination"]["type"] == "page_number"
    assert payload["api_source"]["pagination"]["page_size"] == 50


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
    assert payload.get("source") is None
    assert payload.get("api_source") is None
    assert payload["database_source"]["driver"] == "mongodb"
    assert payload["database_source"]["database"] == "analytics"
    assert payload["database_source"]["table"] == "events"
    assert payload["database_source"]["flatten_nested"] is True


def test_build_omnibeam_manifest_bigquery_export_sink() -> None:
    builder = OmniBeamManifestBuilder()
    storage_src = builder.build_storage_source(
        paths=["gs://lakehouse/gold/sales.parquet"],
        format="csv",
    )
    bq_dest = builder.build_bigquery_destination(
        project_id="my-gcp-project",
        dataset_id="analytics_gold",
        table_id="sales_summary",
        write_disposition="WRITE_TRUNCATE",
        batch_size=1000,
    )
    manifest = builder.build(
        pipeline_id="pipe-export-bq",
        run_id="run-exp-01",
        pipeline_type="sql",
        source_config=storage_src,
        destination_config=bq_dest,
        quarantine_path="gs://lakehouse/dlq/sales",
    )
    payload = json.loads(manifest.to_json())
    assert payload["destination"]["type"] == "bigquery"
    assert payload["destination"]["bigquery_options"]["project_id"] == "my-gcp-project"
    assert payload["destination"]["bigquery_options"]["dataset_id"] == "analytics_gold"
    assert payload["destination"]["bigquery_options"]["table_id"] == "sales_summary"
    assert payload["destination"]["bigquery_options"]["write_disposition"] == "WRITE_TRUNCATE"
    assert payload["destination"]["bigquery_options"]["batch_size"] == 1000


def test_build_omnibeam_manifest_rest_api_export_sink() -> None:
    builder = OmniBeamManifestBuilder()
    storage_src = builder.build_storage_source(
        paths=["gs://lakehouse/gold/notifications.parquet"],
        format="csv",
    )
    api_dest = builder.build_rest_api_destination(
        base_url="https://webhook.site",
        resource_path="/api/v1/notify",
        method="POST",
        batch_size=50,
        credential_ref="secret/webhook-token",
        auth_type="bearer_token",
        headers={"Content-Type": "application/json"},
        body_envelope="events",
    )
    manifest = builder.build(
        pipeline_id="pipe-export-api",
        run_id="run-exp-02",
        pipeline_type="api",
        source_config=storage_src,
        destination_config=api_dest,
        quarantine_path="gs://lakehouse/dlq/notifications",
    )
    payload = json.loads(manifest.to_json())
    assert payload["destination"]["type"] == "rest_api"
    assert payload["destination"]["endpoint"]["base_url"] == "https://webhook.site"
    assert payload["destination"]["endpoint"]["auth_type"] == "bearer_token"
    assert payload["destination"]["api_options"]["resource_path"] == "/api/v1/notify"
    assert payload["destination"]["api_options"]["batch_size"] == 50
    assert payload["destination"]["api_options"]["body_envelope"] == "events"


def test_build_omnibeam_manifest_encryption_and_format_options() -> None:
    builder = OmniBeamManifestBuilder()
    storage_src = builder.build_storage_source(
        paths=["/data/input/transactions.csv"],
        format="csv",
    )
    enc = OmniBeamEncryptionConfig(type="pgp", public_key_ref="secret/pgp-key")
    fmt = OmniBeamFormatOptions(delimiter=";", include_header=True, quote_all=True)
    dest = builder.build_storage_destination(
        output_path="/data/output/encrypted_transactions.csv.pgp",
        output_format="csv",
        encryption=enc,
        format_options=fmt,
    )
    manifest = builder.build(
        pipeline_id="pipe-encrypt",
        run_id="run-enc-01",
        source_config=storage_src,
        destination_config=dest,
        quarantine_path="/data/dlq",
    )
    payload = json.loads(manifest.to_json())
    assert payload["destination"]["encryption"]["type"] == "pgp"
    assert payload["destination"]["encryption"]["public_key_ref"] == "secret/pgp-key"
    assert payload["destination"]["format_options"]["delimiter"] == ";"
    assert payload["destination"]["format_options"]["include_header"] is True
    assert payload["destination"]["format_options"]["quote_all"] is True


def test_build_omnibeam_manifest_secrets_config() -> None:
    builder = OmniBeamManifestBuilder()
    storage_src = builder.build_storage_source(paths=["/data/file.csv"], format="csv")
    secrets = OmniBeamSecretsConfig(
        provider="openbao",
        vault_url="http://openbao:8200",
        vault_token="root",
        gcp_project_id="gcp-test",
        host_override={"db.prod": "db.local"},
    )
    manifest = builder.build(
        pipeline_id="pipe-sec",
        run_id="run-sec-01",
        source_config=storage_src,
        output_path="/data/out",
        quarantine_path="/data/dlq",
        secrets_config=secrets,
    )
    payload = json.loads(manifest.to_json())
    assert payload["secrets_config"]["provider"] == "openbao"
    assert payload["secrets_config"]["vault_url"] == "http://openbao:8200"
    assert payload["secrets_config"]["host_override"]["db.prod"] == "db.local"


def test_build_omnibeam_manifest_quality_rules() -> None:
    builder = OmniBeamManifestBuilder()
    storage_src = builder.build_storage_source(paths=["/data/file.csv"], format="csv")
    rules = [
        OmniBeamQualityRule(type="not_null", column="order_id"),
        OmniBeamQualityRule(
            type="accepted_values", column="status", values=["PENDING", "COMPLETED"]
        ),
        OmniBeamQualityRule(type="row_count_min", value=10),
    ]
    manifest = builder.build(
        pipeline_id="pipe-rules",
        run_id="run-rules-01",
        source_config=storage_src,
        output_path="/data/out",
        quarantine_path="/data/dlq",
        quality_rules=rules,
    )
    payload = json.loads(manifest.to_json())
    assert len(payload["quality_config"]["rules"]) == 3
    assert payload["quality_config"]["rules"][1]["values"] == ["PENDING", "COMPLETED"]
    assert payload["quality_config"]["rules"][2]["value"] == 10


def test_omnibeam_manifest_mutual_exclusion_validation() -> None:
    builder = OmniBeamManifestBuilder()
    storage_src = builder.build_storage_source(paths=["/data/file.csv"], format="csv")
    db_source = builder.build_database_source(driver="postgres", table="users")

    dest = OmniBeamStorageDestinationConfig(output_path="/out")
    dlq = OmniBeamDlqConfig(quarantine_path="/dlq")

    # 1. Zero sources -> Error
    with pytest.raises(ValidationError, match="Exactly one source must be populated"):
        OmniBeamManifest(
            pipeline_id="pipe-err",
            run_id="run-01",
            destination=dest,
            dlq_config=dlq,
        )

    # 2. Multiple sources -> Error
    with pytest.raises(ValidationError, match="Sources are mutually exclusive"):
        OmniBeamManifest(
            pipeline_id="pipe-err",
            run_id="run-02",
            source=storage_src,
            database_source=db_source,
            destination=dest,
            dlq_config=dlq,
        )
