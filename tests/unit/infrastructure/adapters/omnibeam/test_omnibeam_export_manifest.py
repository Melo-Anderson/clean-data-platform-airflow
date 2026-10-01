from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from app.infrastructure.adapters.omnibeam.omnibeam_manifest_builder import (
    OmniBeamManifestBuilder,
)
from app.infrastructure.adapters.omnibeam.omnibeam_manifest_schema import (
    OmniBeamEncryptionConfig,
    OmniBeamFormatOptions,
)


def test_export_storage_sink_parquet_snappy():
    builder = OmniBeamManifestBuilder()
    src = builder.build_storage_source(paths=["gs://lake/gold/orders.parquet"])
    dest = builder.build_storage_destination(
        output_path="gs://lake/export/orders.parquet",
        output_format="parquet",
        compression="snappy",
        single_file=True,
        include_audit_columns=True,
    )
    manifest = builder.build(
        pipeline_id="exp-p01",
        run_id="run-exp-01",
        source_config=src,
        destination_config=dest,
        quarantine_path="gs://lake/dlq",
    )
    data = json.loads(manifest.to_json())
    assert data["destination"]["type"] == "storage"
    assert data["destination"]["output_path"] == "gs://lake/export/orders.parquet"
    assert data["destination"]["output_format"] == "parquet"
    assert data["destination"]["compression"] == "snappy"
    assert data["destination"]["single_file"] is True
    assert data["destination"]["include_audit_columns"] is True


def test_export_storage_sink_delimited_csv_pgp_encryption():
    builder = OmniBeamManifestBuilder()
    src = builder.build_storage_source(paths=["gs://lake/gold/payroll.parquet"])
    enc = OmniBeamEncryptionConfig(type="pgp", public_key_ref="secret/partner-pgp-key")
    fmt = OmniBeamFormatOptions(
        delimiter=";",
        include_header=True,
        charset="iso-8859-1",
        line_terminator="\r\n",
        quote_all=True,
    )
    dest = builder.build_storage_destination(
        output_path="s3://partner-sftp/payroll_2026.csv.pgp",
        output_format="csv",
        encryption=enc,
        format_options=fmt,
    )
    manifest = builder.build(
        pipeline_id="exp-p02",
        run_id="run-exp-02",
        source_config=src,
        destination_config=dest,
        quarantine_path="s3://partner-sftp/dlq",
    )
    data = json.loads(manifest.to_json())
    assert data["destination"]["encryption"]["type"] == "pgp"
    assert data["destination"]["encryption"]["public_key_ref"] == "secret/partner-pgp-key"
    assert data["destination"]["format_options"]["delimiter"] == ";"
    assert data["destination"]["format_options"]["include_header"] is True
    assert data["destination"]["format_options"]["charset"] == "iso-8859-1"
    assert data["destination"]["format_options"]["line_terminator"] == "\r\n"
    assert data["destination"]["format_options"]["quote_all"] is True


def test_export_storage_sink_kms_encryption_validation():
    # KMS requires kms_key_ref
    with pytest.raises(ValidationError, match="requires 'kms_key_ref'"):
        OmniBeamEncryptionConfig(type="kms")

    # PGP requires public_key_ref
    with pytest.raises(ValidationError, match="requires 'public_key_ref'"):
        OmniBeamEncryptionConfig(type="pgp")


def test_export_bigquery_sink_options():
    builder = OmniBeamManifestBuilder()
    src = builder.build_storage_source(paths=["gs://lake/gold/aggregations.parquet"])
    dest = builder.build_bigquery_destination(
        project_id="gcp-analytics-prod",
        dataset_id="dwh_marts",
        table_id="finance_monthly",
        write_disposition="WRITE_TRUNCATE",
        batch_size=2000,
    )
    manifest = builder.build(
        pipeline_id="exp-p03",
        run_id="run-exp-03",
        pipeline_type="sql",
        source_config=src,
        destination_config=dest,
        quarantine_path="gs://lake/dlq",
    )
    data = json.loads(manifest.to_json())
    assert data["destination"]["type"] == "bigquery"
    assert data["destination"]["bigquery_options"]["project_id"] == "gcp-analytics-prod"
    assert data["destination"]["bigquery_options"]["dataset_id"] == "dwh_marts"
    assert data["destination"]["bigquery_options"]["table_id"] == "finance_monthly"
    assert data["destination"]["bigquery_options"]["write_disposition"] == "WRITE_TRUNCATE"
    assert data["destination"]["bigquery_options"]["batch_size"] == 2000


def test_export_rest_api_webhook_sink():
    builder = OmniBeamManifestBuilder()
    src = builder.build_storage_source(paths=["gs://lake/gold/crm_sync.parquet"])
    dest = builder.build_rest_api_destination(
        base_url="https://api.salesforce.com",
        resource_path="/services/data/v58.0/composite/sobjects",
        method="POST",
        batch_size=200,
        credential_ref="secret/salesforce-oauth",
        auth_type="bearer_token",
        headers={"Content-Type": "application/json", "Sforce-Auto-Assign": "FALSE"},
        body_envelope="records",
        rate_limit_rps=25,
        timeout_ms=15000,
        max_retries=5,
    )
    manifest = builder.build(
        pipeline_id="exp-p04",
        run_id="run-exp-04",
        pipeline_type="api",
        source_config=src,
        destination_config=dest,
        quarantine_path="gs://lake/dlq",
    )
    data = json.loads(manifest.to_json())
    assert data["destination"]["type"] == "rest_api"
    assert data["destination"]["endpoint"]["base_url"] == "https://api.salesforce.com"
    assert data["destination"]["endpoint"]["auth_type"] == "bearer_token"
    assert data["destination"]["endpoint"]["headers"]["Sforce-Auto-Assign"] == "FALSE"
    assert data["destination"]["api_options"]["method"] == "POST"
    assert data["destination"]["api_options"]["batch_size"] == 200
    assert data["destination"]["api_options"]["body_envelope"] == "records"
    assert data["destination"]["api_options"]["rate_limit_rps"] == 25
    assert data["destination"]["api_options"]["timeout_ms"] == 15000
    assert data["destination"]["api_options"]["max_retries"] == 5
