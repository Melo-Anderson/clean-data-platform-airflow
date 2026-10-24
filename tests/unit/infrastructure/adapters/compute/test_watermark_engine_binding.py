from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.infrastructure.adapters.compute.duckdb_compute_adapter import DuckDbComputeAdapter
from app.infrastructure.adapters.compute.rest_api_compute_adapter import RestApiComputeAdapter


def test_duckdb_adapter_custom_query_binds_placeholder(tmp_path: Path) -> None:
    secret_mgr = MagicMock()
    creds = {
        "driver": "postgres",
        "host": "localhost",
        "port": 5432,
        "database": "db",
        "user": "usr",
        "password": "pwd",
    }
    secret_mgr.resolve = AsyncMock(return_value=creds)
    adapter = DuckDbComputeAdapter(secret_manager=secret_mgr, output_base_dir=tmp_path)

    config = {
        "source_type": "postgres",
        "credential_ref": "vault:db",
        "extraction_query": "SELECT * FROM orders WHERE id > :watermark_value",
        "watermark_column": "id",
        "watermark_type": "integer",
        "watermark_value": "500",
    }
    with patch("duckdb.connect") as mock_duckdb:
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [("id", "INTEGER")]
        mock_duckdb.return_value = mock_conn

        with patch(
            "app.infrastructure.adapters.compute.duckdb_compute_adapter.calculate_parquet_metrics"
        ) as mock_metrics:
            mock_metrics.return_value = {"row_count": 1}
            adapter._run_extraction(
                job_id="job-duck-1",
                config=config,
                output_dir=tmp_path,
            )
            # Find the COPY query executed
            copy_call = [
                call for call in mock_conn.execute.call_args_list if "COPY (SELECT" in str(call)
            ][0]
            executed_sql = copy_call[0][0]
            assert "WHERE id > 500" in executed_sql


def test_duckdb_adapter_auto_mode_injects_predicate(tmp_path: Path) -> None:
    secret_mgr = MagicMock()
    creds = {
        "driver": "postgres",
        "host": "localhost",
        "port": 5432,
        "database": "db",
        "user": "usr",
        "password": "pwd",
    }
    secret_mgr.resolve = AsyncMock(return_value=creds)
    adapter = DuckDbComputeAdapter(secret_manager=secret_mgr, output_base_dir=tmp_path)

    config = {
        "source_type": "postgres",
        "credential_ref": "vault:db",
        "object_name": "orders",
        "watermark_column": "id",
        "watermark_type": "integer",
        "watermark_value": "100",
    }
    with patch("duckdb.connect") as mock_duckdb:
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [("id", "INTEGER")]
        mock_duckdb.return_value = mock_conn

        with patch(
            "app.infrastructure.adapters.compute.duckdb_compute_adapter.calculate_parquet_metrics"
        ) as mock_metrics:
            mock_metrics.return_value = {"row_count": 1}
            adapter._run_extraction(
                job_id="job-duck-2",
                config=config,
                output_dir=tmp_path,
            )
            copy_call = [
                call for call in mock_conn.execute.call_args_list if "COPY (SELECT" in str(call)
            ][0]
            executed_sql = copy_call[0][0]
            assert "WHERE id > 100" in executed_sql


@pytest.mark.asyncio
async def test_rest_api_adapter_binds_query_params(tmp_path: Path) -> None:
    secret_mgr = MagicMock()
    secret_mgr.resolve = AsyncMock(return_value={"base_url": "https://api.test", "token": "tok"})
    adapter = RestApiComputeAdapter(secret_manager=secret_mgr)

    config = {
        "credential_ref": "vault:api",
        "endpoint": "/events",
        "query_params": {"since": ":watermark_value", "limit": "50"},
        "watermark_column": "ts",
        "watermark_type": "timestamp",
        "watermark_value": "2026-10-24T10:00:00",
    }

    with patch.object(adapter, "_fetch_page", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.return_value = {"data": []}
        await adapter._extract_async(
            job_id="job-rest-1",
            config=config,
            output_dir=tmp_path,
        )
        assert mock_fetch.call_args is not None
        called_params = (
            mock_fetch.call_args[0][2]
            if len(mock_fetch.call_args[0]) > 2
            else mock_fetch.call_args.kwargs.get("params")
        )
        assert called_params["since"] == "2026-10-24T10:00:00"


@pytest.mark.asyncio
async def test_rest_api_adapter_missing_placeholder_raises_error(tmp_path: Path) -> None:
    secret_mgr = MagicMock()
    secret_mgr.resolve = AsyncMock(return_value={"base_url": "https://api.test", "token": "tok"})
    adapter = RestApiComputeAdapter(secret_manager=secret_mgr)

    config = {
        "credential_ref": "vault:api",
        "endpoint": "/events",
        "query_params": {"limit": "50"},  # Missing :watermark_value
        "watermark_column": "ts",
        "watermark_type": "timestamp",
        "watermark_value": "2026-10-24T10:00:00",
    }

    with pytest.raises(ValueError, match="requires ':watermark_value' placeholder in query_params"):
        await adapter._extract_async(
            job_id="job-rest-err",
            config=config,
            output_dir=tmp_path,
        )
