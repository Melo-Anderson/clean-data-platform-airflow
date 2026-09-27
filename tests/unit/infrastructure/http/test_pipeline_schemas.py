import pytest
from pydantic import ValidationError

from app.infrastructure.http.schemas.pipeline_schemas import (
    AirflowConfigRequest,
    ComputeConfigRequest,
    CreatePipelineRequest,
    DestinationObjectRequest,
    ExtractionObjectRequest,
    PipelineResponse,
)


def test_create_pipeline_request_accepts_full_spec() -> None:
    payload = {
        "name": "ingest_orders",
        "pipeline_type": "ingestion",
        "owner_email": "eng@co.com",
        "source_asset_name": "asset-001",
        "cron_schedule": "0 * * * *",
        "source_objects": [
            {
                "object_name": "demo_orders",
                "load_strategy": "full_load",
                "page_size": 1000,
                "compression": "snappy",
                "encoding": "utf-8",
                "extraction_query": "SELECT id, amount FROM demo_orders WHERE amount > 0",
            }
        ],
        "compute": {
            "engine": "duckdb",
            "staging_bucket": "gs://staging-bucket",
            "num_workers": 1,
            "machine_type": "n1-standard-2",
        },
        "quality_rules": [{"type": "not_null", "column": "id"}],
    }
    req = CreatePipelineRequest(**payload)
    assert req.source_objects is not None
    assert (
        req.source_objects[0].extraction_query
        == "SELECT id, amount FROM demo_orders WHERE amount > 0"
    )
    assert req.compute is not None
    assert req.compute.engine == "duckdb"
    assert req.quality_rules is not None
    assert req.quality_rules[0].type == "not_null"


def test_extraction_object_requires_explicit_fields() -> None:
    """ExtractionObjectRequest must fail fast when required fields are missing."""
    with pytest.raises(ValidationError) as exc_info:
        ExtractionObjectRequest(object_name="orders")  # type: ignore[call-arg]

    errors = exc_info.value.errors()
    missing_fields = {e["loc"][0] for e in errors}
    assert {"load_strategy", "page_size", "compression", "encoding"}.issubset(missing_fields)


def test_compute_config_requires_explicit_fields() -> None:
    """ComputeConfigRequest must fail fast when fields are missing (no magic defaults)."""
    with pytest.raises(ValidationError) as exc_info:
        ComputeConfigRequest(engine="duckdb")  # type: ignore[call-arg]

    errors = exc_info.value.errors()
    missing_fields = {e["loc"][0] for e in errors}
    assert {"staging_bucket", "num_workers", "machine_type"}.issubset(missing_fields)


def test_airflow_config_requires_explicit_fields() -> None:
    """AirflowConfigRequest must fail fast when fields are missing (no magic defaults)."""
    with pytest.raises(ValidationError) as exc_info:
        AirflowConfigRequest()  # type: ignore[call-arg]

    errors = exc_info.value.errors()
    missing_fields = {e["loc"][0] for e in errors}
    assert {
        "retries",
        "retry_delay_minutes",
        "execution_timeout_minutes",
        "sla_minutes",
        "tags",
        "pool",
    }.issubset(missing_fields)


def test_destination_object_requires_explicit_fields() -> None:
    """DestinationObjectRequest must fail fast when create_if_not_exists is missing."""
    with pytest.raises(ValidationError) as exc_info:
        DestinationObjectRequest(object_name="orders")  # type: ignore[call-arg]

    errors = exc_info.value.errors()
    missing_fields = {e["loc"][0] for e in errors}
    assert "create_if_not_exists" in missing_fields


def test_pipeline_response_requires_source_asset_name() -> None:
    """PipelineResponse must require source_asset_name without empty string default."""
    with pytest.raises(ValidationError) as exc_info:
        PipelineResponse(  # type: ignore[call-arg]
            id="pipe-1",
            name="orders_pipe",
            pipeline_type="ingestion",
            owner_email="eng@co.com",
        )

    errors = exc_info.value.errors()
    missing_fields = {e["loc"][0] for e in errors}
    assert "source_asset_name" in missing_fields
