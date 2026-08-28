from __future__ import annotations

from datetime import UTC, datetime

from app.domain.pipelines.pipeline_run_file import PipelineRunFile


def test_create_pipeline_run_file() -> None:
    now = datetime.now(tz=UTC)
    run_file = PipelineRunFile(
        id="file-001",
        pipeline_run_id="run-100",
        file_path="/var/data/orders_20260827.csv",
        file_name="orders_20260827.csv",
        file_size_bytes=1024,
        mtime=now,
        hash_md5="d41d8cd98f00b204e9800998ecf8427e",
    )
    assert run_file.id == "file-001"
    assert run_file.pipeline_run_id == "run-100"
    assert run_file.file_name == "orders_20260827.csv"
    assert run_file.status == "PENDING"
    assert run_file.processed_at is None

    run_file.mark_processed()
    assert run_file.status == "PROCESSED"
    assert run_file.processed_at is not None

    run_file.mark_failed()
    assert run_file.status == "FAILED"
