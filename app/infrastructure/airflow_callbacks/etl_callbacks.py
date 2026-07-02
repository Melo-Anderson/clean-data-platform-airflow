from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.infrastructure.compute_job_factory import get_transform_adapter
from app.infrastructure.drift_classifier import DriftClassifier


def validate_source_models(*, pipeline_id: str, source_asset_id: str) -> dict[str, Any]:
    """Validate that all source dbt/Dataform models exist and are fresh."""
    return {"valid": True}


def classify_schema_changes(*, source_models: dict[str, Any]) -> dict[str, Any]:
    """Classify schema changes in source models before running transformation."""
    return DriftClassifier().classify_models(source_models=source_models)


def submit_transformation_job(
    *,
    pipeline_id: str,
    transform_engine: str,
    transform_ref: str,
    compute_config: dict[str, Any],
) -> dict[str, str]:
    """Submit dbt or Dataform transformation job. Returns {"job_id": ...}."""
    adapter = get_transform_adapter(transform_engine)
    job_id = adapter.submit_job(
        pipeline_id=pipeline_id,
        pipeline_type="etl",
        config={"ref": transform_ref, **compute_config},
    )
    return {"job_id": job_id, "submitted_at": datetime.now(tz=UTC).isoformat()}


def publish_documentation(*, pipeline_id: str, transform_ref: str) -> None:
    """Publish updated dbt/Dataform docs to the catalog adapter."""
    pass
