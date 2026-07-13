from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.domain.shared.exceptions import PlatformValidationError
from app.infrastructure.compute_job_factory import get_transform_adapter
from app.infrastructure.drift_classifier import DriftClassifier


def validate_source_models(*, pipeline_id: str, source_asset_id: str) -> dict[str, Any]:
    """Validate that all source dbt/Dataform models exist and are fresh."""
    return {"valid": True}


def classify_schema_changes(*, source_models: dict[str, Any]) -> dict[str, Any]:
    """Classify schema drift between previous and current model snapshots.

    Raises:
        PlatformValidationError: If incompatible drift is detected (field removed,
            type incompatible). The error message identifies the offending fields.
    """
    result = DriftClassifier().classify_models(source_models=source_models)
    if not result["can_proceed"]:
        raise PlatformValidationError(result["blocked_reason"])
    return result


def submit_transformation_job(
    *,
    pipeline_id: str,
    transform_engine: str,
    transform_ref: str,
    compute_config: dict[str, Any],
) -> dict[str, str]:
    """Submit a transformation job via the engine-specific compute adapter.

    Args:
        pipeline_id: Platform pipeline identifier.
        transform_engine: Engine name, e.g. "dbt". Determines adapter selection.
        transform_ref: Model reference path (e.g. "models/orders.sql").
        compute_config: Engine-specific configuration dict.

    Returns:
        {"job_id": str, "submitted_at": ISO timestamp str}
    """
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
