from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.infrastructure.compute_job_factory import get_compute_adapter
from app.infrastructure.drift_classifier import DriftClassifier
from app.infrastructure.platform_client import get_platform_client


def validate_source_and_discovery(
    *,
    pipeline_id: str,
    asset_id: str,
    discovery_config: dict[str, Any],
) -> dict[str, Any]:
    """
    Validate source availability and execute Discovery metadata scan.
    Returns {"available": True, "schema_snapshot": {...}, "drift_detected": bool}.
    """
    get_platform_client()
    # Mock result for stub client
    return {
        "available": True,
        "schema_snapshot": {},
        "drift_detected": False,
    }


def classify_changes_and_plan_actions(
    *,
    schema_snapshot: dict[str, Any],
    on_critical_change: str,
) -> dict[str, Any]:
    """
    Classify schema drift per spec 4.2 (informative vs critical changes).
    """
    classifier = DriftClassifier()
    result = classifier.classify(schema_snapshot=schema_snapshot, policy=on_critical_change)
    if not result["can_proceed"]:
        raise RuntimeError(f"Extraction blocked by schema drift: {result['blocked_reason']}")
    return result


def submit_compute_job(
    *,
    pipeline_id: str,
    source_objects: list[dict[str, Any]],
    compute_config: dict[str, Any],
    staging_bucket: str,
) -> dict[str, str]:
    """
    Submit the compute extraction job asynchronously.
    """
    adapter = get_compute_adapter(compute_config["engine"])
    job_id = adapter.submit_job(
        pipeline_id=pipeline_id,
        pipeline_type="ingestion",
        config={
            "source_objects": source_objects,
            "staging_bucket": staging_bucket,
            **compute_config,
        },
    )
    return {"job_id": job_id, "submitted_at": datetime.now(tz=UTC).isoformat()}


def load_to_data_warehouse(
    *,
    pipeline_id: str,
    destination_object_ids: list[str],
    staging_path: str,
    schema_path: str,
    engine_type: str,
    file_format: str = "parquet",
    connection_metadata: dict[str, Any] | None = None,
    auth_method: str = "iam",
    credential_ref: str | None = None,
) -> dict[str, Any]:
    """Load structured output from compute engine into the data warehouse.

    Resolves Vault credentials if auth_method="vault" before instantiating the loader.
    Delegates the physical load to the correct DwhLoaderAdapter via get_dwh_loader factory.
    """
    effective_metadata: dict[str, Any] = connection_metadata or {}

    resolved_credentials: dict[str, Any] | None = None
    if auth_method == "vault" and credential_ref:
        # Retrieves rotated credentials from OpenBao at runtime — never at compile-time.
        client = get_platform_client()
        resolved_credentials = client.resolve_vault_secrets(credential_ref)

    from app.infrastructure.dwh_loaders.dwh_loader_factory import get_dwh_loader

    loader = get_dwh_loader(engine_type)
    result = loader.load(
        staging_path=staging_path,
        schema_path=schema_path,
        file_format=file_format,
        connection_metadata=effective_metadata,
        resolved_credentials=resolved_credentials,
    )
    return {"loaded": True, "rows_loaded": result.rows_loaded, "engine": result.engine}


def post_load_validation(
    *,
    pipeline_id: str,
    expected_rows: int,
    actual_rows: int,
    source_checksum: str | None,
    destination_checksum: str | None,
) -> dict[str, Any]:
    """Validate volume and checksum integrity after DW load.

    Fails if the row count variation exceeds 0.5% (delta_pct > 0.005) or
    if the source and destination checksums diverge.
    """
    delta_pct = 0.0
    if expected_rows > 0:
        delta_pct = abs(actual_rows - expected_rows) / expected_rows
        if delta_pct > 0.005:
            raise RuntimeError(
                f"post_load_validation failed: expected {expected_rows} rows, "
                f"got {actual_rows} ({delta_pct:.1%} delta exceeds 0.5% threshold)."
            )
    if source_checksum and destination_checksum and source_checksum != destination_checksum:
        raise RuntimeError(
            "post_load_validation failed: checksum mismatch between source and destination."
        )
    return {"validation_ok": True, "row_delta_pct": delta_pct}
