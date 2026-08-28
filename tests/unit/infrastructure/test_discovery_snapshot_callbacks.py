from unittest.mock import MagicMock, patch

from app.infrastructure.airflow_callbacks.ingestion_callbacks import (
    submit_compute_job,
    validate_source_and_discovery,
)
from app.infrastructure.platform_client import PlatformApiClient


def test_platform_client_get_latest_discovery_snapshot():
    client = PlatformApiClient(base_url="http://test-server")
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "object_id": "asset-otg-bronze.transactions",
        "fields": [
            {"name": "transaction_id", "normalized_type": "string", "nullable": False},
            {"name": "player_id", "normalized_type": "string", "nullable": False},
            {"name": "amount", "normalized_type": "decimal", "nullable": True},
        ],
    }

    with patch.object(client, "_get_client") as mock_get_client:
        mock_http = MagicMock()
        mock_http.get.return_value = mock_resp
        mock_get_client.return_value.__enter__.return_value = mock_http

        snapshot = client.get_latest_discovery_snapshot("OTG_bronze", "transactions")
        assert snapshot["object_id"] == "asset-otg-bronze.transactions"
        assert len(snapshot["fields"]) == 3
        assert snapshot["fields"][0]["name"] == "transaction_id"


def test_validate_source_and_discovery_fetches_real_snapshot():
    mock_snapshot = {
        "object_id": "asset-otg-bronze.transactions",
        "fields": [{"name": "transaction_id", "normalized_type": "string", "nullable": False}],
    }

    with patch(
        "app.infrastructure.airflow_callbacks.ingestion_callbacks.get_platform_client"
    ) as mock_get_client:
        mock_client = MagicMock()
        mock_client.get_latest_discovery_snapshot.return_value = mock_snapshot
        mock_get_client.return_value = mock_client

        res = validate_source_and_discovery(
            pipeline_id="p-123",
            asset_id="OTG_bronze",
            discovery_config={"enabled": True, "on_critical_change": "block"},
        )
        assert res["available"] is True
        assert res["schema_snapshot"] == mock_snapshot


def test_submit_compute_job_passes_schema_snapshot_to_adapter():
    mock_adapter = MagicMock()
    mock_adapter.submit_job.return_value = "job-456"

    with patch(
        "app.infrastructure.airflow_callbacks.ingestion_callbacks.get_compute_adapter",
        return_value=mock_adapter,
    ):
        schema_snapshot = {
            "fields": [{"name": "transaction_id", "normalized_type": "string", "nullable": False}]
        }
        res = submit_compute_job(
            pipeline_id="p-123",
            source_objects=[{"object_id": "asset-otg-bronze.transactions"}],
            compute_config={"engine": "omnibeam"},
            staging_bucket="",
            schema_snapshot=schema_snapshot,
        )
        assert res["job_id"] == "job-456"
        call_config = mock_adapter.submit_job.call_args.kwargs["config"]
        assert call_config["schema_snapshot"] == schema_snapshot
