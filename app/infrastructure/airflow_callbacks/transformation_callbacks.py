from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from app.infrastructure.adapters.compute.dbt_compute_adapter import DbtComputeAdapter

logger = logging.getLogger(__name__)


def run_dbt_transformation_job(
    pipeline_id: str,
    project_dir: str = "/opt/airflow/dbt_project",
    profiles_dir: str = "/opt/airflow/dbt_project",
    output_base_dir: str = "/opt/airflow/logs/dbt_outputs",
    select_models: str = "",
) -> dict[str, Any]:
    adapter = DbtComputeAdapter(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        output_base_dir=output_base_dir,
    )
    job_id = adapter.submit_job(
        pipeline_id=pipeline_id,
        pipeline_type="transformation",
        config={"select": select_models},
    )
    result = adapter.poll_job_status(job_id)
    logger.info(
        "dbt transformation job completed: pipeline_id=%s, status=%s, job_id=%s",
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


def evaluate_dbt_quality_gates(
    pipeline_id: str,
    metrics_path: str,
    quality_rules: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    metrics_file = Path(metrics_path)
    if not metrics_file.exists():
        raise FileNotFoundError(f"Metrics file not found at {metrics_path}")

    metrics = json.loads(metrics_file.read_text(encoding="utf-8"))
    tests_failed = metrics.get("tests_failed", 0)
    tests_passed = metrics.get("tests_passed", 0)

    logger.info(
        "dbt quality gates evaluated: pipeline_id=%s, passed=%d, failed=%d",
        pipeline_id,
        tests_passed,
        tests_failed,
    )

    if tests_failed > 0:
        raise RuntimeError(
            f"Quality Gate Failed for transformation pipeline '{pipeline_id}': {tests_failed} dbt tests failed."
        )

    return {
        "quality_ok": True,
        "tests_passed": tests_passed,
        "tests_failed": tests_failed,
    }


def sync_dbt_catalog_metadata(
    asset_id: str,
    manifest_path: str = "/opt/airflow/dbt_project/target/manifest.json",
) -> dict[str, Any]:
    m_file = Path(manifest_path)
    if not m_file.exists():
        logger.warning("dbt catalog sync skipped: manifest not found at %s", manifest_path)
        return {"synced": False, "reason": "Manifest file not found"}

    try:
        import asyncio

        from app.infrastructure.adapters.dbt.dbt_catalog_adapter import DbtCatalogAdapter
        from app.infrastructure.adapters.dbt.dbt_manifest_parser import DbtManifestParser
        from app.infrastructure.persistence.database import get_session_factory
        from app.infrastructure.persistence.sql_unit_of_work import SqlUnitOfWork

        parser = DbtManifestParser()
        manifest = parser.parse_file(m_file)

        uow = SqlUnitOfWork(get_session_factory())
        adapter = DbtCatalogAdapter(uow=uow)
        sync_result = asyncio.run(adapter.sync_manifest(asset_id=asset_id, manifest=manifest))

        logger.info(
            "dbt catalog synced: asset_id=%s, objects=%d, elements=%d",
            asset_id,
            sync_result.objects_synced,
            sync_result.elements_synced,
        )

        return {
            "synced": True,
            "objects_synced": sync_result.objects_synced,
            "elements_synced": sync_result.elements_synced,
        }
    except Exception as exc:
        logger.error("dbt catalog sync error: %s", exc)
        return {"synced": False, "reason": str(exc)}
