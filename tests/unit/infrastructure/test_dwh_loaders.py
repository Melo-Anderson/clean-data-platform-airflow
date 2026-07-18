from __future__ import annotations

import pytest

from app.infrastructure.airflow_callbacks.dwh_loader_adapter import DwhLoaderAdapter, DwhLoadResult
from app.infrastructure.dwh_loaders.bigquery_loader import BigQueryDwhLoader
from app.infrastructure.dwh_loaders.databricks_loader import DatabricksDwhLoader
from app.infrastructure.dwh_loaders.dwh_loader_factory import get_dwh_loader
from app.infrastructure.dwh_loaders.snowflake_loader import SnowflakeDwhLoader


def test_factory_returns_bigquery_loader() -> None:
    loader = get_dwh_loader("bigquery")
    assert isinstance(loader, BigQueryDwhLoader)
    assert isinstance(loader, DwhLoaderAdapter)


def test_factory_returns_databricks_loader() -> None:
    loader = get_dwh_loader("databricks")
    assert isinstance(loader, DatabricksDwhLoader)


def test_factory_returns_snowflake_loader() -> None:
    loader = get_dwh_loader("snowflake")
    assert isinstance(loader, SnowflakeDwhLoader)


def test_factory_is_case_insensitive() -> None:
    loader = get_dwh_loader("BigQuery")
    assert isinstance(loader, BigQueryDwhLoader)


def test_factory_raises_for_unsupported_engine() -> None:
    with pytest.raises(ValueError, match="Unsupported DWH Loader engine: oracle"):
        get_dwh_loader("oracle")


def test_bigquery_loader_load_returns_dwh_load_result() -> None:
    loader = BigQueryDwhLoader()
    result = loader.load(
        staging_path="/tmp/out.parquet",
        schema_path="/tmp/schema.json",
        file_format="parquet",
        connection_metadata={"dataset": "ds", "table": "tbl"},
    )
    assert isinstance(result, DwhLoadResult)
    assert result.engine == "bigquery"


def test_databricks_loader_load_returns_dwh_load_result() -> None:
    loader = DatabricksDwhLoader()
    result = loader.load(
        staging_path="/tmp/out.parquet",
        schema_path="/tmp/schema.json",
        file_format="avro",
        connection_metadata={"warehouse": "wh", "table": "tbl"},
    )
    assert isinstance(result, DwhLoadResult)
    assert result.engine == "databricks"


def test_snowflake_loader_load_returns_dwh_load_result() -> None:
    loader = SnowflakeDwhLoader()
    result = loader.load(
        staging_path="/tmp/out.parquet",
        schema_path="/tmp/schema.json",
        file_format="parquet",
        connection_metadata={"stage": "st", "table": "tbl"},
    )
    assert isinstance(result, DwhLoadResult)
    assert result.engine == "snowflake"
