from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
from pathlib import Path
from typing import Any, cast

from app.application.shared.ports.transformation_catalog_port import (
    TransformationCatalogSyncResult,
)
from app.infrastructure.adapters.transformation.transformation_catalog_registry import (
    TransformationCatalogRegistry,
)
from app.infrastructure.compute_job_factory import get_transform_adapter

logger = logging.getLogger(__name__)


def run_transformation_job(
    pipeline_id: str,
    output_base_dir: str,
    engine: str = "dbt",
    project_dir: str | None = None,
    profiles_dir: str | None = None,
    select_models: str = "",
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Submits a transformation job to the compute adapter and polls to completion.

    Raises:
        ValueError: If output_base_dir is empty.
        KeyError: If engine is not registered in ComputeAdapterRegistry.
    """
    if not output_base_dir.strip():
        raise ValueError("output_base_dir must not be empty for transformation job")

    adapter = get_transform_adapter(engine)
    merged_config: dict[str, Any] = {
        "output_base_dir": output_base_dir,
        "select": select_models,
        **(config or {}),
    }
    if project_dir:
        merged_config["project_dir"] = project_dir
    if profiles_dir:
        merged_config["profiles_dir"] = profiles_dir

    job_id = adapter.submit_job(
        pipeline_id=pipeline_id,
        pipeline_type="transformation",
        config=merged_config,
    )
    result = adapter.poll_job_status(job_id)
    logger.info(
        "%s transformation job completed: pipeline_id=%s, status=%s, job_id=%s",
        engine,
        pipeline_id,
        result.status.value,
        job_id,
    )
    return {
        "job_id": job_id,
        "status": result.status.value,
        "metrics_path": result.metrics_path,
        "output_path": result.output_path,
    }


def evaluate_transformation_quality_gates(
    pipeline_id: str,
    metrics_path: str,
    quality_rules: list[dict[str, Any]] | None = None,
    engine: str = "dbt",
) -> dict[str, Any]:
    """Evaluates transformation quality metrics.

    Raises:
        FileNotFoundError: If metrics file does not exist.
        KeyError: If metrics file is malformed (missing required counters).
        RuntimeError: If quality tests failed.
    """
    metrics_file = Path(metrics_path)
    if not metrics_file.exists():
        raise FileNotFoundError(f"Metrics file not found at {metrics_path}")

    metrics = json.loads(metrics_file.read_text(encoding="utf-8"))
    if "tests_failed" not in metrics or "tests_passed" not in metrics:
        raise KeyError(
            f"Malformed metrics file at {metrics_path}: "
            f"missing 'tests_failed' or 'tests_passed' keys"
        )

    tests_failed = int(metrics["tests_failed"])
    tests_passed = int(metrics["tests_passed"])
    logger.info(
        "%s quality gates: pipeline_id=%s, passed=%d, failed=%d",
        engine,
        pipeline_id,
        tests_passed,
        tests_failed,
    )

    if tests_failed > 0:
        raise RuntimeError(
            f"Quality Gate Failed for transformation pipeline '{pipeline_id}': "
            f"{tests_failed} transformation quality tests failed."
        )
    return {"quality_ok": True, "tests_passed": tests_passed, "tests_failed": tests_failed}


def _run_coroutine_safe(coro: Any) -> Any:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop is not None and loop.is_running():
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, coro).result()
    return asyncio.run(coro)


def sync_transformation_catalog_metadata(
    asset_id: str,
    manifest_path: str,
    engine: str = "dbt",
) -> dict[str, Any]:
    """Synchronizes transformation catalog metadata.

    Raises:
        ValueError: If engine has no registered catalog adapter.
        FileNotFoundError: If manifest file does not exist.
    """
    adapter = TransformationCatalogRegistry.get(engine)
    sync_result = cast(
        TransformationCatalogSyncResult,
        _run_coroutine_safe(adapter.sync_catalog(asset_id=asset_id, manifest_path=manifest_path)),
    )
    logger.info(
        "%s catalog synced: asset_id=%s, objects=%d, elements=%d",
        engine,
        asset_id,
        sync_result.objects_synced,
        sync_result.elements_synced,
    )
    return sync_result.to_dict()


# --- Backward-compatibility ---


def run_dbt_transformation_job(
    pipeline_id: str,
    project_dir: str = "/opt/airflow/dbt_project",
    profiles_dir: str = "/opt/airflow/dbt_project",
    output_base_dir: str = "/opt/airflow/logs/dbt_outputs",
    select_models: str = "",
) -> dict[str, Any]:
    """Legacy dbt wrapper — delegates to run_transformation_job with engine='dbt'."""
    return run_transformation_job(
        pipeline_id=pipeline_id,
        output_base_dir=output_base_dir,
        engine="dbt",
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        select_models=select_models,
    )


# Direct aliases — same signatures as the generic functions
evaluate_dbt_quality_gates = evaluate_transformation_quality_gates
sync_dbt_catalog_metadata = sync_transformation_catalog_metadata
