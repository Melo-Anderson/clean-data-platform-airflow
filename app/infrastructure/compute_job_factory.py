from __future__ import annotations

from app.config import get_settings
from app.infrastructure.adapters.compute.dbt_compute_adapter import DbtComputeAdapter
from app.infrastructure.adapters.compute.duckdb_compute_adapter import DuckDbComputeAdapter
from app.infrastructure.adapters.compute.omnibeam_compute_adapter import OmniBeamComputeAdapter
from app.infrastructure.adapters.compute.registry import ComputeAdapterRegistry
from app.infrastructure.adapters.compute.rest_api_compute_adapter import RestApiComputeAdapter
from app.infrastructure.adapters.secrets.secret_manager_factory import get_secret_manager
from app.infrastructure.airflow_callbacks.compute_job_adapter import ComputeJobAdapter


def _duckdb_factory() -> ComputeJobAdapter:
    settings = get_settings()
    return DuckDbComputeAdapter(
        secret_manager=get_secret_manager(settings),
        output_base_dir=settings.duckdb_output_dir,
        default_credential_ref=settings.default_postgres_credential_ref,
    )


def _rest_api_factory() -> ComputeJobAdapter:
    settings = get_settings()
    return RestApiComputeAdapter(
        secret_manager=get_secret_manager(settings),
        output_base_dir=settings.rest_api_output_dir,
    )


def _omnibeam_factory() -> ComputeJobAdapter:
    settings = get_settings()
    return OmniBeamComputeAdapter(
        output_base_dir=settings.omnibeam_output_dir,
        binary_path=settings.omnibeam_binary_path,
    )


def _dbt_factory() -> ComputeJobAdapter:
    settings = get_settings()
    return DbtComputeAdapter(
        project_dir=settings.dbt.project_dir,
        profiles_dir=settings.dbt.profiles_dir,
        output_base_dir=settings.dbt.output_base_dir,
    )


ComputeAdapterRegistry.register("duckdb", _duckdb_factory)
ComputeAdapterRegistry.register("rest_api", _rest_api_factory)
ComputeAdapterRegistry.register("omnibeam", _omnibeam_factory)
ComputeAdapterRegistry.register("dbt", _dbt_factory)


def get_compute_adapter(engine: str) -> ComputeJobAdapter:
    return ComputeAdapterRegistry.get(engine)


def get_transform_adapter(engine: str) -> ComputeJobAdapter:
    return ComputeAdapterRegistry.get(engine)
