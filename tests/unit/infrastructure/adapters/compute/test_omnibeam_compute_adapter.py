from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.infrastructure.adapters.compute.omnibeam_compute_adapter import (
    OmniBeamComputeAdapter,
)
from app.infrastructure.airflow_callbacks.compute_job_adapter import (
    JobStatus,
)
from app.infrastructure.compute_job_factory import get_compute_adapter


@pytest.fixture
def mock_executor(tmp_path: Path):
    def fake_run_command(cmd: list[str], output_dir: Path) -> int:
        (output_dir / "metrics.json").write_text(
            json.dumps({"row_count": 150, "null_count": 0}), encoding="utf-8"
        )
        (output_dir / "data.parquet").write_bytes(b"PAR1fake")
        return 0

    return fake_run_command


def test_omnibeam_compute_adapter_submit_and_poll(tmp_path: Path, mock_executor) -> None:
    adapter = OmniBeamComputeAdapter(output_base_dir=str(tmp_path), executor_fn=mock_executor)
    config = {
        "manifest_json": json.dumps({"pipeline_id": "p1", "run_id": "r1"}),
    }

    job_id = adapter.submit_job(pipeline_id="p1", pipeline_type="ingestion", config=config)
    result = adapter.poll_job_status(job_id)

    assert result.status == JobStatus.SUCCESS
    assert result.job_id == job_id
    assert Path(result.output_path).exists()
    assert Path(result.metrics_path).exists()


def test_omnibeam_compute_adapter_failure(tmp_path: Path) -> None:
    def fake_failed_executor(cmd: list[str], output_dir: Path) -> int:
        (output_dir / "error.txt").write_text("Execution failed", encoding="utf-8")
        return 1

    adapter = OmniBeamComputeAdapter(
        output_base_dir=str(tmp_path), executor_fn=fake_failed_executor
    )
    config = {"manifest_json": "{}"}

    job_id = adapter.submit_job(pipeline_id="p2", pipeline_type="ingestion", config=config)
    result = adapter.poll_job_status(job_id)

    assert result.status == JobStatus.FAILED
    assert result.error_message == "Execution failed"


def test_compute_job_factory_returns_omnibeam_adapter() -> None:
    adapter = get_compute_adapter("omnibeam")
    assert isinstance(adapter, OmniBeamComputeAdapter)


def test_omnibeam_compute_adapter_direct_binary(tmp_path: Path) -> None:
    captured_commands: list[list[str]] = []

    def mock_binary_executor(cmd: list[str], output_dir: Path) -> int:
        captured_commands.append(cmd)
        (output_dir / "metrics.json").write_text(json.dumps({"row_count": 10}), encoding="utf-8")
        (output_dir / "data.parquet").write_bytes(b"PAR1")
        return 0

    fake_binary = tmp_path / "bin" / "omnibeam"
    fake_binary.parent.mkdir()
    fake_binary.write_text("#!/bin/sh\nexit 0", encoding="utf-8")

    adapter = OmniBeamComputeAdapter(
        output_base_dir=str(tmp_path / "outputs"),
        binary_path=str(fake_binary),
        executor_fn=mock_binary_executor,
    )
    job_id = adapter.submit_job("pipe-bin", "ingestion", {"manifest_json": "{}"})
    assert job_id is not None
    assert len(captured_commands) == 1
    assert captured_commands[0][0] == str(fake_binary)

    assert "--config_payload_path=" in captured_commands[0][1]


def test_omnibeam_adapter_fails_fast_when_binary_missing(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"

    def missing_binary_executor(cmd: list[str], output_dir: Path) -> int:
        (output_dir / "error.txt").write_text(
            "Executable not found: omnibeam-pipeline", encoding="utf-8"
        )
        return 1

    adapter = OmniBeamComputeAdapter(
        output_base_dir=str(out_dir),
        binary_path="non_existent_binary",
        executor_fn=missing_binary_executor,
    )

    job_id = adapter.submit_job(
        "test-pipeline",
        "ingestion",
        config={"source_type": "storage", "paths": ["/data/dummy.csv"], "format": "csv"},
    )
    result = adapter.poll_job_status(job_id)

    assert result.status == JobStatus.FAILED
    assert result.error_message is not None
    assert "Executable not found" in result.error_message

    # Garante que nenhum parquet fictício foi criado para mascarar o erro
    job_matches = list(out_dir.glob(f"**/{job_id}"))
    assert job_matches, "Job directory must exist"
    job_dir = job_matches[0]
    assert not list(job_dir.glob("*.parquet*")), "No fake parquet must be created on failure"


def test_omnibeam_adapter_accepts_explicit_constructor_args(tmp_path: Path) -> None:
    """Constructor must accept explicit args — no internal get_settings() call."""
    adapter = OmniBeamComputeAdapter(
        output_base_dir=str(tmp_path / "out"),
        binary_path="my-custom-binary",
    )
    assert adapter._binary_path == "my-custom-binary"


class MockSecretManager:
    def __init__(self, secrets: dict[str, dict[str, str]] | None = None) -> None:
        self._secrets = secrets or {}

    async def resolve(self, ref: str) -> dict[str, str]:
        if ref not in self._secrets:
            raise KeyError(f"Secret not found: {ref}")
        return self._secrets[ref]


def test_omnibeam_adapter_database_source_with_secret_resolution(
    tmp_path: Path, mock_executor
) -> None:
    secrets = {
        "secret/postgres": {
            "driver": "postgres",
            "connection_uri": "postgresql://prod_user:prod_password_123@postgres.corp:5432/corp_db?sslmode=disable",
            "schema": "demo",
        }
    }
    secret_mgr = MockSecretManager(secrets)
    adapter = OmniBeamComputeAdapter(
        output_base_dir=str(tmp_path / "outputs"),
        secret_manager=secret_mgr,
        executor_fn=mock_executor,
    )
    config = {
        "source_type": "database",
        "object_name": "demo.customers",
        "credential_ref": "secret/postgres",
        "schema_snapshot": {"fields": [{"name": "id", "type": "int"}]},
    }
    job_id = adapter.submit_job("pipe-cust", "ingestion", config)
    manifest_file = tmp_path / "outputs" / "pipe-cust" / job_id / "manifest.json"
    assert manifest_file.exists()
    payload = json.loads(manifest_file.read_text(encoding="utf-8"))

    assert payload["database_source"] is not None
    assert payload["database_source"]["driver"] == "postgres"
    assert payload["database_source"]["table"] == "demo.customers"
    assert (
        payload["database_source"]["connection_uri"]
        == "postgresql://prod_user:prod_password_123@postgres.corp:5432/corp_db?sslmode=disable"
    )


def test_omnibeam_adapter_database_source_fallback_to_builder(
    tmp_path: Path, mock_executor
) -> None:
    secrets = {
        "secret/postgres_legacy": {
            "driver": "postgres",
            "user": "prod_user",
            "password": "prod_password_123",
            "host": "postgres.corp",
            "port": "5432",
            "database": "corp_db",
            "schema": "demo",
        }
    }
    secret_mgr = MockSecretManager(secrets)
    adapter = OmniBeamComputeAdapter(
        output_base_dir=str(tmp_path / "outputs"),
        secret_manager=secret_mgr,
        executor_fn=mock_executor,
    )
    config = {
        "source_type": "database",
        "object_name": "customers",
        "credential_ref": "secret/postgres_legacy",
        "schema_snapshot": {"fields": [{"name": "id", "type": "int"}]},
    }
    job_id = adapter.submit_job("pipe-cust-legacy", "ingestion", config)
    manifest_file = tmp_path / "outputs" / "pipe-cust-legacy" / job_id / "manifest.json"
    assert manifest_file.exists()
    payload = json.loads(manifest_file.read_text(encoding="utf-8"))

    assert payload["database_source"] is not None
    assert (
        payload["database_source"]["connection_uri"]
        == "postgresql://prod_user:prod_password_123@postgres.corp:5432/corp_db"
    )


def test_omnibeam_adapter_database_fail_fast_when_secret_fails(
    tmp_path: Path, mock_executor
) -> None:
    secret_mgr = MockSecretManager({})
    adapter = OmniBeamComputeAdapter(
        output_base_dir=str(tmp_path / "outputs"),
        secret_manager=secret_mgr,
        executor_fn=mock_executor,
    )
    config = {
        "source_type": "database",
        "object_name": "customers",
        "credential_ref": "secret/nonexistent",
    }
    with pytest.raises(ValueError, match="Failed to resolve required database credentials"):
        adapter.submit_job("pipe-fail", "ingestion", config)


def test_omnibeam_adapter_mongodb_source(tmp_path: Path, mock_executor) -> None:
    secrets = {
        "secret/mongo": {
            "connection_uri": "mongodb://m_user:m_pass@mongo.corp:27017/analytics?authSource=admin",
            "database": "analytics",
            "collection": "events",
        }
    }
    secret_mgr = MockSecretManager(secrets)
    adapter = OmniBeamComputeAdapter(
        output_base_dir=str(tmp_path / "outputs"),
        secret_manager=secret_mgr,
        executor_fn=mock_executor,
    )
    config = {
        "source_type": "mongodb",
        "collection": "events",
        "credential_ref": "secret/mongo",
        "schema_snapshot": {"fields": [{"name": "event_id", "type": "string"}]},
    }
    job_id = adapter.submit_job("pipe-mongo", "ingestion", config)
    manifest_file = tmp_path / "outputs" / "pipe-mongo" / job_id / "manifest.json"
    assert manifest_file.exists()
    payload = json.loads(manifest_file.read_text(encoding="utf-8"))

    assert payload["database_source"] is not None
    assert payload["database_source"]["driver"] == "mongodb"
    assert payload["database_source"]["table"] == "events"
    assert (
        payload["database_source"]["connection_uri"]
        == "mongodb://m_user:m_pass@mongo.corp:27017/analytics?authSource=admin"
    )


def test_omnibeam_adapter_rest_api_source(tmp_path: Path, mock_executor) -> None:
    secret_mgr = MockSecretManager(
        {
            "secret/api": {
                "base_url": "https://api.corp.local",
                "auth_type": "bearer",
            }
        }
    )
    adapter = OmniBeamComputeAdapter(
        output_base_dir=str(tmp_path / "outputs"),
        secret_manager=secret_mgr,
        executor_fn=mock_executor,
    )
    config = {
        "source_type": "rest_api",
        "credential_ref": "secret/api",
        "endpoint": "/v1/orders",
        "schema_snapshot": {"fields": [{"name": "order_id", "type": "string"}]},
    }
    job_id = adapter.submit_job("pipe-api", "ingestion", config)
    manifest_file = tmp_path / "outputs" / "pipe-api" / job_id / "manifest.json"
    assert manifest_file.exists()
    payload = json.loads(manifest_file.read_text(encoding="utf-8"))

    assert payload["api_source"] is not None
    assert payload["api_source"]["base_url"] == "https://api.corp.local"
    assert payload["api_source"]["endpoint"] == "/v1/orders"


def test_omnibeam_adapter_rest_api_fail_fast_missing_base_url(
    tmp_path: Path, mock_executor
) -> None:
    secret_mgr = MockSecretManager({"secret/api-empty": {}})
    adapter = OmniBeamComputeAdapter(
        output_base_dir=str(tmp_path / "outputs"),
        secret_manager=secret_mgr,
        executor_fn=mock_executor,
    )
    config = {
        "source_type": "rest_api",
        "credential_ref": "secret/api-empty",
        "endpoint": "/v1/orders",
    }
    with pytest.raises(ValueError, match="base_url is required"):
        adapter.submit_job("pipe-api-fail", "ingestion", config)


def test_omnibeam_adapter_storage_source(tmp_path: Path, mock_executor) -> None:
    sample_file = tmp_path / "data.csv"
    sample_file.write_text("id,name\n1,alice\n", encoding="utf-8")
    adapter = OmniBeamComputeAdapter(
        output_base_dir=str(tmp_path / "outputs"),
        executor_fn=mock_executor,
    )
    config = {
        "source_type": "storage",
        "paths": [str(sample_file)],
        "format": "csv",
        "schema_snapshot": {"fields": [{"name": "id", "type": "int"}]},
    }
    job_id = adapter.submit_job("pipe-storage", "ingestion", config)
    manifest_file = tmp_path / "outputs" / "pipe-storage" / job_id / "manifest.json"
    assert manifest_file.exists()
    payload = json.loads(manifest_file.read_text(encoding="utf-8"))

    assert payload["source"] is not None
    assert payload["source"]["type"] == "storage"
    assert payload["source"]["paths"] == [str(sample_file)]
    assert payload["source"]["format"] == "csv"


def test_omnibeam_adapter_storage_empty_paths(tmp_path: Path, mock_executor) -> None:
    adapter = OmniBeamComputeAdapter(
        output_base_dir=str(tmp_path / "outputs"),
        executor_fn=mock_executor,
    )
    config = {
        "source_type": "storage",
        "paths": [],
        "format": "csv",
    }
    job_id = adapter.submit_job("pipe-storage-empty", "ingestion", config)
    manifest_file = tmp_path / "outputs" / "pipe-storage-empty" / job_id / "manifest.json"
    assert manifest_file.exists()
    payload = json.loads(manifest_file.read_text(encoding="utf-8"))
    assert payload["source"]["paths"] == []


def test_omnibeam_adapter_database_raises_when_driver_missing(
    tmp_path: Path, mock_executor
) -> None:
    secrets = {
        "secret/custom_db": {
            "user": "prod_user",
            "password": "prod_password_123",
            "host": "db.corp",
            "database": "corp_db",
        }
    }
    secret_mgr = MockSecretManager(secrets)
    adapter = OmniBeamComputeAdapter(
        output_base_dir=str(tmp_path / "outputs"),
        secret_manager=secret_mgr,
        executor_fn=mock_executor,
    )
    config = {
        "source_type": "database",
        "object_name": "customers",
        "credential_ref": "secret/custom_db",
    }
    with pytest.raises(ValueError, match="driver"):
        adapter.submit_job("pipe-cust", "ingestion", config)


def test_omnibeam_adapter_passes_standard_postgres_uri(tmp_path: Path, mock_executor) -> None:
    secrets = {
        "secret/postgres": {
            "driver": "postgres",
            "connection_uri": "postgresql://airflow:airflow@postgres:5432/platform_db?sslmode=disable",
            "database": "platform_db",
        }
    }
    secret_mgr = MockSecretManager(secrets)
    adapter = OmniBeamComputeAdapter(
        output_base_dir=str(tmp_path / "outputs"),
        secret_manager=secret_mgr,
        executor_fn=mock_executor,
    )
    config = {
        "source_type": "database",
        "driver": "postgres",
        "source_objects": [{"object_name": "demo.orders"}],
        "credential_ref": "secret/postgres",
    }
    job_id = adapter.submit_job("pipe-orders", "ingestion", config)
    manifest_file = tmp_path / "outputs" / "pipe-orders" / job_id / "manifest.json"
    assert manifest_file.exists()
    payload = json.loads(manifest_file.read_text(encoding="utf-8"))

    assert payload["database_source"]["driver"] == "postgres"
    assert (
        payload["database_source"]["connection_uri"]
        == "postgresql://airflow:airflow@postgres:5432/platform_db?sslmode=disable"
    )
    assert payload["database_source"]["database"] == "platform_db"
    assert payload["database_source"]["table"] == "demo.orders"


def test_omnibeam_adapter_infers_mongodb_from_driver(tmp_path: Path, mock_executor) -> None:
    secrets = {
        "secret/mongo": {
            "driver": "mongodb",
            "connection_uri": "mongodb://admin:password@mongodb:27017/test_db?authSource=admin",
            "database": "test_db",
        }
    }
    secret_mgr = MockSecretManager(secrets)
    adapter = OmniBeamComputeAdapter(
        output_base_dir=str(tmp_path / "outputs"),
        secret_manager=secret_mgr,
        executor_fn=mock_executor,
    )
    config = {
        "source_type": "mongodb",
        "driver": "mongodb",
        "source_objects": [{"object_name": "user_events"}],
        "credential_ref": "secret/mongo",
    }
    job_id = adapter.submit_job("p_ingest_user_events", "ingestion", config)
    manifest_file = tmp_path / "outputs" / "p_ingest_user_events" / job_id / "manifest.json"
    assert manifest_file.exists()
    payload = json.loads(manifest_file.read_text(encoding="utf-8"))

    assert payload["source"]["type"] == "mongodb"
    assert payload["source"]["database"] == "test_db"
    assert payload["source"]["collection"] == "user_events"


def test_omnibeam_adapter_storage_with_dict_files(tmp_path: Path, mock_executor) -> None:
    sample_file = tmp_path / "players.json"
    sample_file.write_text('{"id": 1, "name": "Neymar"}', encoding="utf-8")

    adapter = OmniBeamComputeAdapter(
        output_base_dir=str(tmp_path / "outputs"),
        executor_fn=mock_executor,
    )
    config = {
        "source_type": "storage",
        "files": [{"file_path": str(sample_file), "file_name": "players.json"}],
        "format": "json",
    }
    job_id = adapter.submit_job("pipe-dict-files", "ingestion", config)
    manifest_file = tmp_path / "outputs" / "pipe-dict-files" / job_id / "manifest.json"
    assert manifest_file.exists()
    payload = json.loads(manifest_file.read_text(encoding="utf-8"))

    assert payload["source"]["type"] == "storage"
    assert payload["source"]["paths"] == [sample_file.as_posix()]
    assert payload["source"]["format"] == "json"


def test_omnibeam_adapter_storage_requires_explicit_format(tmp_path: Path, mock_executor) -> None:
    adapter = OmniBeamComputeAdapter(
        output_base_dir=str(tmp_path / "outputs"),
        executor_fn=mock_executor,
    )
    config = {
        "source_type": "storage",
        "paths": ["/some/path.csv"],
    }
    with pytest.raises(ValueError, match="format is required for storage pipeline"):
        adapter.submit_job("pipe-no-format", "ingestion", config)
