from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from app.domain.pipelines.pipeline_run import PipelineRun
from app.domain.pipelines.pipeline_run_status import PipelineRunStatus


def _now() -> datetime:
    return datetime.now(tz=UTC)


def _run(**kwargs) -> PipelineRun:
    defaults = dict(
        id=str(uuid.uuid4()),
        pipeline_id="pipe-1",
        pipeline_name="test_pipeline",
        pipeline_type="ingestion",
        dag_run_id="manual__2026-06-30",
        status=PipelineRunStatus.RUNNING,
        started_at=_now(),
    )
    return PipelineRun(**{**defaults, **kwargs})


def test_mark_success_transitions_to_success() -> None:
    run = _run()
    run.mark_success(finished_at=_now(), metrics={"rows_written": 1000})
    assert run.status == PipelineRunStatus.SUCCESS
    assert run.metrics["rows_written"] == 1000
    assert run.finished_at is not None


def test_mark_success_with_optional_failures_transitions_to_partial() -> None:
    run = _run(optional_failures=["emit_raw_lineage", "success_notification"])
    run.mark_success(finished_at=_now(), metrics={})
    assert run.status == PipelineRunStatus.PARTIAL
    assert run.is_partial() is True


def test_mark_failed_records_failed_task() -> None:
    run = _run()
    run.mark_failed(finished_at=_now(), failed_task="load_to_data_warehouse")
    assert run.status == PipelineRunStatus.FAILED
    assert run.failed_task == "load_to_data_warehouse"


def test_mark_quality_failed_records_violations() -> None:
    run = _run()
    run.mark_quality_failed(
        finished_at=_now(),
        violations=["not_null violation on column 'customer_id'"],
    )
    assert run.status == PipelineRunStatus.QUALITY_FAILED
    assert len(run.quality_violations) == 1


def test_duration_seconds_none_when_not_finished() -> None:
    run = _run()
    assert run.duration_seconds() is None


def test_duration_seconds_when_finished() -> None:
    start = _now()
    end = start + timedelta(minutes=5)
    run = _run(started_at=start, finished_at=end, status=PipelineRunStatus.SUCCESS)
    assert run.duration_seconds() == pytest.approx(300.0, abs=1.0)
