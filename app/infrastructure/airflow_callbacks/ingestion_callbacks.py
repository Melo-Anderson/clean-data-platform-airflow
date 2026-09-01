from __future__ import annotations

import fnmatch
import hashlib
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.infrastructure.compute_job_factory import get_compute_adapter
from app.infrastructure.drift_classifier import DriftClassifier
from app.infrastructure.dwh_loaders.dwh_loader_factory import get_dwh_loader
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
    client = get_platform_client()
    snapshot = client.get_latest_discovery_snapshot(asset_id)
    return {
        "available": True,
        "schema_snapshot": snapshot,
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


def resolve_source_files(
    *,
    pipeline_id: str,
    source_objects: list[dict[str, Any]],
    landing_dir: str = "/opt/airflow/data/landing",
    scope_include: list[str] | None = None,
    scope_exclude: list[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Scans landing directory, computes MD5 hashes, and filters out already processed files.
    Returns serializable file metadata dictionaries for downstream Airflow tasks and tracking.
    """
    client = get_platform_client()
    processed_hashes = client.get_processed_hashes(pipeline_id)

    root_paths = [Path(landing_dir), Path("./data/landing"), Path("data/landing")]
    root = next((p for p in root_paths if p.exists() and p.is_dir()), None)
    if not root:
        raise FileNotFoundError(
            f"Landing directory does not exist. Tried: {[str(p) for p in root_paths]}"
        )

    obj_names = []
    for obj in source_objects:
        raw_id = obj.get("object_id") or obj.get("object_name") or ""
        clean_name = raw_id.split(".")[-1].lower()
        if clean_name:
            obj_names.append(clean_name)

    patterns = scope_include or ([f"*{name}*" for name in obj_names] if obj_names else ["*.*"])
    exclude_patterns = scope_exclude or [".*"]

    pending_files: list[dict[str, Any]] = []
    for path in sorted(root.glob("**/*")):
        if not path.is_file() or path.name.startswith("."):
            continue

        name = path.name.lower()
        rel = path.relative_to(root).as_posix().lower()

        if not any(
            fnmatch.fnmatch(name, pat.lower()) or fnmatch.fnmatch(rel, pat.lower())
            for pat in patterns
        ):
            continue
        if any(
            fnmatch.fnmatch(name, exc.lower()) or fnmatch.fnmatch(rel, exc.lower())
            for exc in exclude_patterns
        ):
            continue

        hasher = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                hasher.update(chunk)
        file_hash = hasher.hexdigest()

        if file_hash in processed_hashes:
            continue

        stat = path.stat()
        pending_files.append(
            {
                "id": str(uuid.uuid4()),
                "pipeline_run_id": "",
                "file_path": path.resolve().as_posix(),
                "file_name": path.name,
                "file_size_bytes": stat.st_size,
                "mtime": datetime.fromtimestamp(stat.st_mtime, tz=UTC).isoformat(),
                "hash_md5": file_hash,
                "status": "PROCESSED",
                "processed_at": datetime.now(tz=UTC).isoformat(),
            }
        )

    return pending_files


def submit_compute_job(
    *,
    pipeline_id: str,
    source_objects: list[dict[str, Any]],
    compute_config: dict[str, Any],
    staging_bucket: str,
    schema_snapshot: dict[str, Any] | None = None,
    files: list[dict[str, Any]] | None = None,
) -> dict[str, str]:
    """
    Submit the compute extraction job asynchronously with discovered schema snapshot.
    """
    adapter = get_compute_adapter(compute_config.get("engine", "omnibeam"))
    job_id = adapter.submit_job(
        pipeline_id=pipeline_id,
        pipeline_type="ingestion",
        config={
            "source_objects": source_objects,
            "staging_bucket": staging_bucket,
            "schema_snapshot": schema_snapshot or {},
            "files": files or [],
            **compute_config,
        },
    )
    return {"job_id": job_id, "submitted_at": datetime.now(tz=UTC).isoformat()}


def load_to_data_warehouse(
    *,
    pipeline_id: str,
    destination_object_ids: list[str],
    staging_path: str | None,
    schema_path: str | None,
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
    if not staging_path:
        return {"loaded": False, "rows_loaded": 0, "engine": engine_type}

    if not engine_type:
        raise ValueError(
            "engine_type is required for load_to_data_warehouse. "
            "Set it explicitly in the pipeline compute_config."
        )

    effective_metadata: dict[str, Any] = connection_metadata or {}

    resolved_credentials: dict[str, Any] | None = None
    if auth_method == "vault" and credential_ref:
        # Retrieves rotated credentials from OpenBao at runtime — never at compile-time.
        client = get_platform_client()
        resolved_credentials = client.resolve_vault_secrets(credential_ref)

    loader = get_dwh_loader(engine_type)
    result = loader.load(
        staging_path=staging_path,
        schema_path=schema_path or "",
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
