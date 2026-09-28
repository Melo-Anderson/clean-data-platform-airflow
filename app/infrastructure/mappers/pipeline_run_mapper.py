# app/infrastructure/mappers/pipeline_run_mapper.py
from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from app.domain.pipelines.pipeline_run import PipelineRun
from app.domain.pipelines.pipeline_run_file import PipelineRunFile
from app.domain.shared.platform_defaults import PlatformDefaults

_DEFAULTS = PlatformDefaults()


def _fmt_dt(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        return value
    return None


def serialize_pipeline_run_file(f: dict[str, Any] | PipelineRunFile) -> dict[str, Any]:
    """Serialize a physical file record for HTTP transport."""
    if isinstance(f, dict):
        return {
            "id": f.get("id") or str(uuid.uuid4()),
            "file_path": f.get("file_path", ""),
            "file_name": f.get("file_name", ""),
            "file_size_bytes": f.get("file_size_bytes", 0),
            "mtime": _fmt_dt(f.get("mtime")) or datetime.now(tz=UTC).isoformat(),
            "hash_md5": f.get("hash_md5", ""),
            "status": f.get("status", "PROCESSED"),
            "processed_at": _fmt_dt(f.get("processed_at")),
        }
    return {
        "id": f.id,
        "file_path": f.file_path,
        "file_name": f.file_name,
        "file_size_bytes": f.file_size_bytes,
        "mtime": _fmt_dt(f.mtime) or datetime.now(tz=UTC).isoformat(),
        "hash_md5": f.hash_md5,
        "status": f.status,
        "processed_at": _fmt_dt(f.processed_at),
    }


def serialize_pipeline_run_dict(run: dict[str, Any]) -> dict[str, Any]:
    """Serialize a dictionary pipeline run (e.g. Airflow callbacks) for HTTP transport."""
    return {
        "id": run.get("id") or str(uuid.uuid4()),
        "pipeline_id": run.get("pipeline_id", ""),
        "pipeline_name": run.get("pipeline_name", ""),
        "pipeline_type": run.get("pipeline_type", "ingestion"),
        "dag_run_id": run.get("dag_run_id", "unknown"),
        "status": run.get("status", "success"),
        "started_at": _fmt_dt(run.get("started_at")) or datetime.now(tz=UTC).isoformat(),
        "finished_at": _fmt_dt(run.get("finished_at")),
        "failed_task": run.get("failed_task"),
        "optional_failures": run.get("optional_failures") or [],
        "quality_violations": run.get("quality_violations") or [],
        "metrics": run.get("metrics") or {},
        "sla_minutes": run.get("sla_minutes", _DEFAULTS.airflow.sla_minutes),
        "sla_breached": run.get("sla_breached", False),
        "files": [serialize_pipeline_run_file(f) for f in (run.get("files") or [])],
    }


def serialize_pipeline_run_entity(
    run: PipelineRun, files: list[PipelineRunFile] | None = None
) -> dict[str, Any]:
    """Serialize a PipelineRun domain entity for HTTP transport."""
    return {
        "id": run.id,
        "pipeline_id": run.pipeline_id,
        "pipeline_name": run.pipeline_name,
        "pipeline_type": run.pipeline_type,
        "dag_run_id": run.dag_run_id,
        "status": run.status.value,
        "started_at": _fmt_dt(run.started_at),
        "finished_at": _fmt_dt(run.finished_at),
        "failed_task": run.failed_task,
        "optional_failures": run.optional_failures or [],
        "quality_violations": run.quality_violations or [],
        "metrics": run.metrics or {},
        "sla_minutes": run.sla_minutes,
        "sla_breached": run.sla_breached,
        "files": [serialize_pipeline_run_file(f) for f in (files or [])],
    }


def serialize_pipeline_run(run: dict[str, Any] | PipelineRun) -> dict[str, Any]:
    """Convenience mapper dispatching to typed functions."""
    if isinstance(run, dict):
        return serialize_pipeline_run_dict(run)
    return serialize_pipeline_run_entity(run)
