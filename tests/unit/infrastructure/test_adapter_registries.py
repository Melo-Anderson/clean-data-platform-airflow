from typing import Any

import pytest

from app.config import Settings
from app.infrastructure.adapters.compute.registry import ComputeAdapterRegistry
from app.infrastructure.airflow_callbacks.compute_job_adapter import (
    ComputeJobResult,
    JobStatus,
)
from app.infrastructure.compute_job_factory import get_compute_adapter, get_transform_adapter
from app.infrastructure.dwh_loaders.dwh_loader_factory import get_dwh_loader
from app.infrastructure.dwh_loaders.registry import DwhLoaderRegistry
from app.infrastructure.dwh_provisioners.dwh_provisioner_factory import get_dwh_provisioner


class CustomMockEngine:
    def submit_job(self, pipeline_id: str, pipeline_type: str, config: dict[str, Any]) -> str:
        return "custom-job-1"

    def poll_job_status(self, job_id: str) -> ComputeJobResult:
        return ComputeJobResult(job_id=job_id, status=JobStatus.SUCCESS)

    def cancel_job(self, job_id: str) -> None:
        pass


class CustomMockLoader:
    async def load(self, source_path: str, destination_ref: str, mode: str = "overwrite") -> None:
        pass


def test_compute_adapter_registry_resolves_standard_engines() -> None:
    duckdb_adapter = get_compute_adapter("duckdb")
    assert duckdb_adapter is not None

    omnibeam_adapter = get_compute_adapter("omnibeam")
    assert omnibeam_adapter is not None

    dbt_adapter = get_transform_adapter("dbt")
    assert dbt_adapter is not None


def test_compute_adapter_registry_custom_registration() -> None:
    ComputeAdapterRegistry.register("custom_spark", lambda: CustomMockEngine())
    adapter = ComputeAdapterRegistry.get("custom_spark")
    assert adapter.submit_job("p1", "ingestion", {}) == "custom-job-1"


def test_dwh_loader_registry_resolves_standard_loaders() -> None:
    assert get_dwh_loader("bigquery") is not None
    assert get_dwh_loader("databricks") is not None
    assert get_dwh_loader("snowflake") is not None
    assert get_dwh_loader("noop") is not None


def test_dwh_loader_registry_custom_registration() -> None:
    DwhLoaderRegistry.register("custom_redshift", lambda: CustomMockLoader())
    loader = DwhLoaderRegistry.get("custom_redshift")
    assert loader is not None


def test_dwh_loader_registry_raises_on_unsupported() -> None:
    with pytest.raises(ValueError, match="Unsupported DWH Loader engine"):
        get_dwh_loader("non_existent_engine")


def test_dwh_provisioner_registry_resolves_standard() -> None:
    s_noop = Settings(dwh_provisioner_adapter="noop")
    assert get_dwh_provisioner(s_noop) is not None

    s_bq = Settings(dwh_provisioner_adapter="bigquery", gcp_project="test-prj")
    assert get_dwh_provisioner(s_bq) is not None
