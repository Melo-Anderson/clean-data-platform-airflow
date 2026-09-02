from __future__ import annotations

from app.config import get_settings
from app.infrastructure.airflow_callbacks.dwh_loader_adapter import DwhLoaderAdapter
from app.infrastructure.dwh_loaders.bigquery_loader import BigQueryDwhLoader
from app.infrastructure.dwh_loaders.databricks_loader import DatabricksDwhLoader
from app.infrastructure.dwh_loaders.noop_loader import NoOpDwhLoader
from app.infrastructure.dwh_loaders.registry import DwhLoaderRegistry
from app.infrastructure.dwh_loaders.snowflake_loader import SnowflakeDwhLoader

DwhLoaderRegistry.register(
    "bigquery",
    lambda: BigQueryDwhLoader(
        project=get_settings().dwh.gcp_project,
        credentials_path=get_settings().dwh.resolved_credentials_path,
    ),
)
DwhLoaderRegistry.register("databricks", lambda: DatabricksDwhLoader())
DwhLoaderRegistry.register("snowflake", lambda: SnowflakeDwhLoader())
DwhLoaderRegistry.register("noop", lambda: NoOpDwhLoader())


def get_dwh_loader(engine_type: str) -> DwhLoaderAdapter:
    """Instantiates the correct DwhLoaderAdapter based on the target engine using DwhLoaderRegistry.

    Args:
        engine_type: Engine name (case-insensitive). Supported: bigquery, databricks, snowflake, noop.

    Raises:
        ValueError: If the engine doesn't have a registered loader.
    """
    return DwhLoaderRegistry.get(engine_type)
