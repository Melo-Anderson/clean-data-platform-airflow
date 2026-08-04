from __future__ import annotations

from unittest.mock import MagicMock

from app.infrastructure.dwh_loaders.bigquery_loader import BigQueryDwhLoader


def test_bigquery_dwh_loader_executes_load_table_from_uri():
    mock_client = MagicMock()
    mock_client.project = "my-project"
    mock_job = MagicMock()
    mock_job.output_rows = 150
    mock_client.load_table_from_uri.return_value = mock_job

    loader = BigQueryDwhLoader(client=mock_client)
    res = loader.load(
        staging_path="gs://my-bucket/staging/customers.parquet",
        schema_path="",
        file_format="parquet",
        connection_metadata={"dataset": "raw", "table": "customers"},
    )

    assert res.rows_loaded == 150
    assert res.engine == "bigquery"
    mock_client.load_table_from_uri.assert_called_once()
    mock_job.result.assert_called_once()

    args, kwargs = mock_client.load_table_from_uri.call_args
    assert args[0] == "gs://my-bucket/staging/customers.parquet"
    assert args[1] == "my-project.raw.customers"
    assert kwargs.get("job_config") is not None
