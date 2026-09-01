from unittest.mock import MagicMock

import pytest

from app.infrastructure.dwh_provisioners.bigquery_provisioner import BigQueryProvisioner


@pytest.mark.asyncio
async def test_bigquery_provisioner_caches_dataset_creation() -> None:
    mock_client = MagicMock()
    mock_client.project = "test-project"
    provisioner = BigQueryProvisioner(client=mock_client, project="test-project")

    # First call: triggers create_dataset
    await provisioner.ensure_dataset_exists("platform_silver", "Silver dataset")
    assert mock_client.create_dataset.call_count == 1

    # Second call within TTL: cached, no API call
    await provisioner.ensure_dataset_exists("platform_silver", "Silver dataset")
    assert mock_client.create_dataset.call_count == 1


@pytest.mark.asyncio
async def test_bigquery_provisioner_caches_table_creation() -> None:
    mock_client = MagicMock()
    mock_client.project = "test-project"
    mock_client.get_table.side_effect = Exception("Table not found")
    provisioner = BigQueryProvisioner(client=mock_client, project="test-project")

    # First call: creates table
    await provisioner.ensure_table_exists("platform_silver", "slv_players")
    assert mock_client.create_table.call_count == 1

    # Second call within TTL: cached, no API call
    await provisioner.ensure_table_exists("platform_silver", "slv_players")
    assert mock_client.create_table.call_count == 1
