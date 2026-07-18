from __future__ import annotations

from app.infrastructure.airflow_callbacks.dwh_loader_adapter import DwhLoaderAdapter
from app.infrastructure.dwh_loaders.bigquery_loader import BigQueryDwhLoader
from app.infrastructure.dwh_loaders.databricks_loader import DatabricksDwhLoader
from app.infrastructure.dwh_loaders.snowflake_loader import SnowflakeDwhLoader


def get_dwh_loader(engine_type: str) -> DwhLoaderAdapter:
    """Instantiates the correct DwhLoaderAdapter based on the target engine.

    Args:
        engine_type: Engine name (case-insensitive). Supported: bigquery, databricks, snowflake.

    Raises:
        ValueError: If the engine doesn't have a registered loader.
    """
    loaders: dict[str, type[DwhLoaderAdapter]] = {
        "bigquery": BigQueryDwhLoader,
        "databricks": DatabricksDwhLoader,
        "snowflake": SnowflakeDwhLoader,
    }
    loader_cls = loaders.get(engine_type.lower())
    if not loader_cls:
        raise ValueError(f"Unsupported DWH Loader engine: {engine_type}")
    return loader_cls()
