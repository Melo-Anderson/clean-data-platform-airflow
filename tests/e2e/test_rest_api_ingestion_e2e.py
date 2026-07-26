# tests/e2e/test_rest_api_ingestion_e2e.py
from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from app.infrastructure.adapters.compute.rest_api_compute_adapter import RestApiComputeAdapter
from app.infrastructure.adapters.secrets.noop_secret_manager_adapter import (
    NoopSecretManagerAdapter,
)
from app.infrastructure.airflow_callbacks.compute_job_adapter import (
    ComputeJobResult,
    JobStatus,
)

pytestmark = pytest.mark.e2e

_CRED_REF = "secret/mock-store"
_MOCK_API_TOKEN = "e2e-test-token"  # mock_store_api does not validate tokens

_in_docker = os.path.exists("/.dockerenv") or os.getenv("API_URL", "").startswith(
    "http://platform-api"
)
_mock_host = os.getenv("MOCK_API_HOST", "mock-api" if _in_docker else "127.0.0.1")


def _base_url() -> str:
    return f"http://{_mock_host}:8081"


def _adapter(tmp_path: Path) -> RestApiComputeAdapter:
    return RestApiComputeAdapter(
        secret_manager=NoopSecretManagerAdapter(store={_CRED_REF: {"token": _MOCK_API_TOKEN}}),
        output_base_dir=str(tmp_path),
    )


def _poll_until_done(adapter: RestApiComputeAdapter, job_id: str) -> ComputeJobResult:  # type: ignore[name-defined]
    for _ in range(30):
        result = adapter.poll_job_status(job_id)
        if result.status != JobStatus.RUNNING:
            return result
        time.sleep(0.5)
    return adapter.poll_job_status(job_id)


def test_rest_api_ingestion_products_single_page(tmp_path: Path) -> None:
    """Extract all products from mock_store_api in a single page and verify Parquet output."""
    adapter = _adapter(tmp_path)

    config = {
        "base_url": _base_url(),
        "resource_path": "/api/v1/products",
        "credential_ref": _CRED_REF,
        "auth_type": "bearer",
        "pagination": {
            "strategy": "none",  # single-page: limit > 15 products in seed
        },
    }

    job_id = adapter.submit_job("pipe-e2e-products", "ingestion", config)
    result = _poll_until_done(adapter, job_id)

    assert result.status == JobStatus.SUCCESS, f"Job failed: {result.error_message}"
    assert result.output_path is not None
    assert result.metrics_path is not None

    # Verify Parquet
    table = pq.read_table(result.output_path)
    assert table.num_rows == 15, f"Expected 15 products (seed data), got {table.num_rows}"
    assert "id" in table.column_names
    assert "name" in table.column_names
    assert "price" in table.column_names

    # Verify metrics
    metrics = json.loads(Path(result.metrics_path).read_text(encoding="utf-8"))
    assert metrics["row_count"] == 15
    assert metrics["pages_fetched"] == 1
    assert metrics["bytes_written"] > 0

    # Verify schema
    assert result.schema_path is not None
    schema = json.loads(Path(result.schema_path).read_text(encoding="utf-8"))
    column_names = {col["column"] for col in schema}
    assert "id" in column_names
    assert "name" in column_names


def test_rest_api_ingestion_customers_multi_page(tmp_path: Path) -> None:
    """Extract customers from mock_store_api using offset_limit pagination (page/limit API)."""
    adapter = _adapter(tmp_path)

    config = {
        "base_url": _base_url(),
        "resource_path": "/api/v1/customers",
        "credential_ref": _CRED_REF,
        "auth_type": "bearer",
        "pagination": {
            "strategy": "page_number",
            "page_size": 8,
            "limit_param": "limit",
            "page_param": "page",
            "page_start": 1,
        },
    }

    job_id = adapter.submit_job("pipe-e2e-customers", "ingestion", config)
    result = _poll_until_done(adapter, job_id)

    assert result.status == JobStatus.SUCCESS, f"Job failed: {result.error_message}"
    assert result.output_path is not None

    # Verify Parquet - 20+ customers in seed data
    table = pq.read_table(result.output_path)
    assert table.num_rows >= 20, f"Expected at least 20 customers (seed data), got {table.num_rows}"
    assert "email" in table.column_names
    assert "full_name" in table.column_names


# Override the module-level e2e marker so it runs without docker if run individually
@pytest.mark.e2e
def test_rest_api_dag_generation() -> None:
    """Pipeline with engine=rest_api must produce valid Airflow DAG referencing rest_api adapter."""
    from app.domain.pipelines.compute_config import ComputeConfig
    from app.domain.pipelines.compute_engine import ComputeEngine
    from app.domain.pipelines.extraction_config import ExtractionConfig
    from app.domain.pipelines.load_strategy import LoadStrategy
    from app.domain.pipelines.pipeline import Pipeline
    from app.domain.pipelines.pipeline_type import PipelineType
    from app.domain.pipelines.schedule_config import ScheduleConfig
    from app.domain.pipelines.schedule_mode import ScheduleMode
    from app.domain.shared.value_objects import CronSchedule, EmailAddress
    from app.infrastructure.dag_generator.ci_validator import CiValidator
    from app.infrastructure.dag_generator.dag_generator import DagGenerator
    from app.infrastructure.yaml_generator.pipeline_yaml_generator import PipelineYamlGenerator

    pipeline = Pipeline(
        id="pipe-rest-ingest",
        name="ingest-api-products",
        type=PipelineType.INGESTION,
        owner=EmailAddress("data-eng@co.com"),
        schedule=ScheduleConfig(mode=ScheduleMode.CRON, cron_schedule=CronSchedule("0 2 * * *")),
        compute=ComputeConfig(engine=ComputeEngine.REST_API, staging_bucket="s3://stage"),
        source_objects=[
            ExtractionConfig(
                object_id="obj-api-products",
                load_strategy=LoadStrategy.FULL_LOAD,
            )
        ],
    )

    yaml_generator = PipelineYamlGenerator()
    yaml_content = yaml_generator.generate(pipeline)

    # Validate YAML is structurally correct
    ci_validator = CiValidator()
    errors = ci_validator.validate_yaml(yaml_content)
    assert not errors, f"YAML validation failed: {errors}"

    # Verify engine is serialized correctly to YAML
    assert "rest_api" in yaml_content

    # Generate DAG code
    dag_generator = DagGenerator()
    dag_code = dag_generator.generate(yaml_content)

    # Verify DAG code structure
    assert "ingest_api_products_dag" in dag_code
    assert '"rest_api"' in dag_code  # engine value in _PIPELINE_PARAMS
    assert 'task_id="submit_compute_job"' in dag_code
    assert 'task_id="monitor_compute_job"' in dag_code
