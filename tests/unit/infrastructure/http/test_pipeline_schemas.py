import pytest
from pydantic import ValidationError

from app.infrastructure.http.schemas.pipeline_schemas import (
    CreatePipelineRequest,
    ExtractionObjectRequest,
)


def test_create_pipeline_request_accepts_full_spec() -> None:
    payload = {
        "name": "ingest_orders",
        "pipeline_type": "ingestion",
        "owner_email": "eng@co.com",
        "source_asset": "asset-001",
        "cron_schedule": "0 * * * *",
        "source_objects": [
            {
                "object_id": "demo_orders",
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
        ExtractionObjectRequest(object_id="orders")  # type: ignore[call-arg]

    errors = exc_info.value.errors()
    missing_fields = {e["loc"][0] for e in errors}
    assert {"load_strategy", "page_size", "compression", "encoding"}.issubset(missing_fields)
