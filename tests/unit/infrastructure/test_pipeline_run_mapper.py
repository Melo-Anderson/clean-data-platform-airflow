# tests/unit/infrastructure/test_pipeline_run_mapper.py
from __future__ import annotations

from datetime import UTC, datetime

from app.domain.pipelines.pipeline_run import PipelineRun
from app.domain.pipelines.pipeline_run_status import PipelineRunStatus
from app.infrastructure.mappers.pipeline_run_mapper import (
    serialize_pipeline_run,
    serialize_pipeline_run_file,
)


def test_serialize_file_from_dict_preserves_fields() -> None:
    now = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    f = {
        "id": "file-1",
        "file_path": "/data/landing/orders.csv",
        "file_name": "orders.csv",
        "file_size_bytes": 1024,
        "mtime": now,
        "hash_md5": "abc123",
        "status": "PROCESSED",
        "processed_at": None,
    }
    result = serialize_pipeline_run_file(f)
    assert result["file_name"] == "orders.csv"
    assert result["hash_md5"] == "abc123"
    assert result["status"] == "PROCESSED"
    assert result["id"] == "file-1"


def test_serialize_file_generates_id_when_none() -> None:
    f = {
        "id": None,
        "file_path": "/data/landing/orders.csv",
        "file_name": "orders.csv",
        "file_size_bytes": 512,
        "mtime": datetime(2026, 1, 1, tzinfo=UTC),
        "hash_md5": "xyz",
        "status": "PROCESSED",
        "processed_at": None,
    }
    result = serialize_pipeline_run_file(f)
    assert result["id"] is not None
    assert len(result["id"]) > 10  # UUID


def test_serialize_pipeline_run_from_entity() -> None:
    now = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    run = PipelineRun(
        id="run-123",
        pipeline_id="pipe-1",
        pipeline_name="order_pipeline",
        pipeline_type="ingestion",
        dag_run_id="dag_run_abc",
        status=PipelineRunStatus.SUCCESS,
        started_at=now,
    )
    result = serialize_pipeline_run(run)
    assert result["id"] == "run-123"
    assert result["pipeline_id"] == "pipe-1"
    assert result["pipeline_name"] == "order_pipeline"
    assert result["status"] == "success"
    assert result["started_at"] == now.isoformat()
    assert result["files"] == []


def test_serialize_pipeline_run_from_dict() -> None:
    now = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    d = {
        "id": "run-456",
        "pipeline_id": "pipe-2",
        "pipeline_name": "customer_pipeline",
        "pipeline_type": "ingestion",
        "dag_run_id": "dag_run_xyz",
        "status": "failed",
        "started_at": now,
        "failed_task": "task_fail",
        "files": [],
    }
    result = serialize_pipeline_run(d)
    assert result["id"] == "run-456"
    assert result["pipeline_id"] == "pipe-2"
    assert result["status"] == "failed"
    assert result["failed_task"] == "task_fail"
