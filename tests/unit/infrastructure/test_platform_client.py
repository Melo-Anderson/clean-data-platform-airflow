from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.domain.pipelines.pipeline_run import PipelineRun
from app.domain.pipelines.pipeline_run_file import PipelineRunFile
from app.domain.pipelines.pipeline_run_status import PipelineRunStatus
from app.infrastructure.platform_client import PlatformApiClient


def test_platform_client_upsert_pipeline_run_calls_http_api() -> None:
    client = PlatformApiClient(base_url="http://mock-api:8000")

    now = datetime.now(tz=UTC)
    file_record = PipelineRunFile(
        id="f1",
        pipeline_run_id="r1",
        file_path="/data/file.csv",
        file_name="file.csv",
        file_size_bytes=500,
        mtime=now,
        hash_md5="md5hash",
        status="PROCESSED",
        processed_at=now,
    )
    run_entity = PipelineRun(
        id="r1",
        pipeline_id="p1",
        pipeline_name="Ingest_Test",
        pipeline_type="ingestion",
        dag_run_id="dag_1",
        status=PipelineRunStatus.SUCCESS,
        started_at=now,
        finished_at=now,
    )
    run_entity.files = [file_record]

    with patch("httpx.Client.post") as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_post.return_value = mock_response

        client.upsert_pipeline_run(run_entity)

        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert args[0] == "/v1/pipelines/p1/runs/record"
        assert kwargs["json"]["id"] == "r1"
        assert kwargs["json"]["pipeline_id"] == "p1"
        assert len(kwargs["json"]["files"]) == 1
        assert kwargs["json"]["files"][0]["file_name"] == "file.csv"


def test_platform_client_pipeline_succeeded_on() -> None:
    client = PlatformApiClient(base_url="http://mock-api:8000")
    now = datetime.now(tz=UTC)

    with patch("httpx.Client.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"success": True, "status": "success"}
        mock_get.return_value = mock_response

        res = client.pipeline_succeeded_on("p1", True, now, "same_day")
        assert res is True
        mock_get.assert_called_once()
        args, kwargs = mock_get.call_args
        assert args[0] == "/v1/pipelines/p1/runs/latest"
        assert kwargs["params"]["require_same_day"] == "true"


def test_platform_client_emit_raw_lineage() -> None:
    client = PlatformApiClient(base_url="http://mock-api:8000")
    with patch("httpx.Client.post") as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        client.emit_raw_lineage("p1", ["obj-1"], ["obj-2"], "/path/to/schema")
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert args[0] == "/v1/lineage/raw"
        assert kwargs["json"]["pipeline_id"] == "p1"
        assert kwargs["json"]["source_object_ids"] == ["obj-1"]
        assert kwargs["json"]["destination_object_ids"] == ["obj-2"]


def test_platform_client_update_freshness_status() -> None:
    client = PlatformApiClient(base_url="http://mock-api:8000")
    with patch("httpx.Client.post") as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        client.update_freshness_status("p1", ["obj-2"])
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert args[0] == "/v1/lineage/freshness"
        assert kwargs["json"]["pipeline_id"] == "p1"
        assert kwargs["json"]["destination_object_ids"] == ["obj-2"]


def test_platform_client_emit_etl_lineage() -> None:
    client = PlatformApiClient(base_url="http://mock-api:8000")
    with patch("httpx.Client.post") as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        client.emit_etl_lineage("p1", "dbt_transform", "/path/to/schema")
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert args[0] == "/v1/lineage/etl"
        assert kwargs["json"]["transform_ref"] == "dbt_transform"


def test_platform_client_emit_export_lineage() -> None:
    client = PlatformApiClient(base_url="http://mock-api:8000")
    with patch("httpx.Client.post") as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        client.emit_export_lineage("p1", ["src-1"], ["dest-1"], None)
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert args[0] == "/v1/lineage/export"


def test_platform_client_execute_sensor_query() -> None:
    client = PlatformApiClient(base_url="http://mock-api:8000")
    with patch("httpx.Client.post") as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"result": [{"count": 10}]}
        mock_post.return_value = mock_response

        res = client.execute_sensor_query("asset-1", "SELECT 1")
        assert res == [{"count": 10}]
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert args[0] == "/v1/assets/asset-1/sensors/query"


def test_platform_client_notify_failure() -> None:
    client = PlatformApiClient(base_url="http://mock-api:8000")
    with patch("httpx.Client.post") as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        client.notify_failure("p1", "extract_step", "Timeout error")
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert args[0] == "/v1/pipelines/p1/notifications/failure"
        assert kwargs["json"]["failed_task"] == "extract_step"
        assert kwargs["json"]["error_message"] == "Timeout error"


def test_platform_client_resolve_vault_secrets_unauthenticated() -> None:
    client = PlatformApiClient(base_url="http://mock-api:8000")
    assert client.resolve_vault_secrets("vault/none") == {}
    assert client.resolve_vault_secrets("") == {}


def test_platform_client_vault_resolution_raises_on_error() -> None:
    client = PlatformApiClient(
        base_url="http://localhost:8000",
        vault_url="http://vault:8200",
        vault_token="test-token",
    )

    with patch(
        "app.infrastructure.platform_client.BaoSecretManagerAdapter.resolve",
        new_callable=AsyncMock,
        side_effect=ConnectionError("Vault unreachable"),
    ):
        with pytest.raises(RuntimeError, match="Could not resolve secret"):
            client.resolve_vault_secrets("secret/db_creds")
