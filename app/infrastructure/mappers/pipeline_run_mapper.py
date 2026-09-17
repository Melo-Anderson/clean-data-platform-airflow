# app/infrastructure/mappers/pipeline_run_mapper.py
from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from app.domain.pipelines.pipeline_run import PipelineRun


def _fmt_dt(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        return value
    return None


def serialize_pipeline_run_file(f: Any) -> dict[str, Any]:
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
        "id": getattr(f, "id", None) or str(uuid.uuid4()),
        "file_path": getattr(f, "file_path", ""),
        "file_name": getattr(f, "file_name", ""),
        "file_size_bytes": getattr(f, "file_size_bytes", 0),
        "mtime": _fmt_dt(getattr(f, "mtime", None)) or datetime.now(tz=UTC).isoformat(),
        "hash_md5": getattr(f, "hash_md5", ""),
        "status": getattr(f, "status", "PROCESSED"),
        "processed_at": _fmt_dt(getattr(f, "processed_at", None)),
    }


def serialize_pipeline_run(run: dict[str, Any] | PipelineRun) -> dict[str, Any]:
    """Serialize a PipelineRun entity or dictionary for HTTP transport."""
    if isinstance(run, dict):
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
            "sla_minutes": run.get("sla_minutes", 90),
            "sla_breached": run.get("sla_breached", False),
            "files": [serialize_pipeline_run_file(f) for f in (run.get("files") or [])],
        }
    files = getattr(run, "files", []) or []
    status_val = run.status.value if hasattr(run.status, "value") else str(run.status)
    return {
        "id": run.id,
        "pipeline_id": run.pipeline_id,
        "pipeline_name": run.pipeline_name,
        "pipeline_type": run.pipeline_type,
        "dag_run_id": run.dag_run_id,
        "status": status_val,
        "started_at": _fmt_dt(run.started_at),
        "finished_at": _fmt_dt(run.finished_at),
        "failed_task": run.failed_task,
        "optional_failures": run.optional_failures or [],
        "quality_violations": run.quality_violations or [],
        "metrics": run.metrics or {},
        "sla_minutes": run.sla_minutes,
        "sla_breached": run.sla_breached,
        "files": [serialize_pipeline_run_file(f) for f in files],
    }
