from __future__ import annotations

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
                "extraction_query": "SELECT id, amount FROM demo_orders WHERE amount > 0",
            }
        ],
        "compute": {"engine": "duckdb"},
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


def test_extraction_object_defaults() -> None:
    obj = ExtractionObjectRequest(object_id="orders")
    assert obj.load_strategy == "full_load"
    assert obj.page_size == 1000
    assert obj.extraction_query is None
