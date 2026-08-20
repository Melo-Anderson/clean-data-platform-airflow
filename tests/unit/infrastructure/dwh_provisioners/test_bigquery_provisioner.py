from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.application.shared.ports.dwh_provisioner_port import DwhProvisionerPort
from app.infrastructure.dwh_provisioners.bigquery_provisioner import BigQueryProvisioner


def test_bigquery_provisioner_implements_protocol():
    mock_client = MagicMock()
    provisioner = BigQueryProvisioner(client=mock_client)
    assert isinstance(provisioner, DwhProvisionerPort)


@pytest.mark.asyncio
async def test_bigquery_provisioner_creates_dataset():
    mock_client = MagicMock()
    mock_client.project = "my-project"
    provisioner = BigQueryProvisioner(client=mock_client)

    await provisioner.ensure_dataset_exists(
        "raw_customers", description="Raw dataset", labels={"env": "prod"}
    )

    mock_client.create_dataset.assert_called_once()
    args, kwargs = mock_client.create_dataset.call_args
    dataset = args[0]
    assert dataset.dataset_id == "raw_customers"
    assert dataset.description == "Raw dataset"
    assert dataset.labels == {"env": "prod"}
    assert kwargs.get("exists_ok") is True


@pytest.mark.asyncio
async def test_bigquery_provisioner_creates_table():
    mock_client = MagicMock()
    mock_client.project = "my-project"
    provisioner = BigQueryProvisioner(client=mock_client)

    await provisioner.ensure_table_exists(
        "raw",
        "customers",
        description="Raw customers table",
        labels={"managed_by": "clean_data_platform"},
        schema_fields=[{"name": "id", "type": "INTEGER", "mode": "REQUIRED"}],
    )

    mock_client.create_table.assert_called_once()
    args, kwargs = mock_client.create_table.call_args
    table = args[0]
    assert table.table_id == "customers"
    assert table.dataset_id == "raw"
    assert table.description == "Raw customers table"
    assert table.labels == {"managed_by": "clean_data_platform"}
    assert len(table.schema) == 1
    assert table.schema[0].name == "id"
    assert table.schema[0].field_type == "INTEGER"
    assert table.schema[0].mode == "REQUIRED"
    assert kwargs.get("exists_ok") is True


@pytest.mark.asyncio
async def test_bigquery_provisioner_sanitizes_labels_with_invalid_chars():
    """Labels com @, . ou outros chars inválidos devem ser sanitizados antes de enviar ao BQ."""
    mock_client = MagicMock()
    mock_client.project = "my-project"
    provisioner = BigQueryProvisioner(client=mock_client)

    await provisioner.ensure_dataset_exists(
        "my_dataset",
        labels={"owner": "data-team@company.com"},
    )

    args, kwargs = mock_client.create_dataset.call_args
    dataset = args[0]
    assert dataset.labels == {"owner": "data-team_at_company_com"}


@pytest.mark.asyncio
async def test_bigquery_provisioner_sanitizes_labels_with_dots():
    mock_client = MagicMock()
    mock_client.project = "my-project"
    provisioner = BigQueryProvisioner(client=mock_client)

    await provisioner.ensure_table_exists(
        "my_dataset",
        "my_table",
        labels={"owner": "team.name.com", "env": "prod"},
    )

    args, kwargs = mock_client.create_table.call_args
    table = args[0]
    assert table.labels == {"owner": "team_name_com", "env": "prod"}


@pytest.mark.asyncio
async def test_bigquery_provisioner_truncates_label_value_to_63_chars():
    mock_client = MagicMock()
    mock_client.project = "my-project"
    provisioner = BigQueryProvisioner(client=mock_client)
    long_value = "a" * 100

    await provisioner.ensure_dataset_exists(
        "my_dataset",
        labels={"key": long_value},
    )

    args, _ = mock_client.create_dataset.call_args
    dataset = args[0]
    assert len(dataset.labels["key"]) <= 63
