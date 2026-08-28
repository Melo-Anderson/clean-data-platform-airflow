from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.application.pipelines.record_pipeline_run_use_case import RecordPipelineRunUseCase
from app.domain.pipelines.pipeline_run_file import PipelineRunFile
from app.domain.pipelines.pipeline_run_status import PipelineRunStatus


@pytest.mark.asyncio
async def test_record_pipeline_run_use_case_saves_run_and_files() -> None:
    uow = MagicMock()
    uow.__aenter__ = AsyncMock(return_value=uow)
    uow.__aexit__ = AsyncMock(return_value=None)
    uow.pipeline_runs = MagicMock()
    uow.pipeline_runs.save = AsyncMock(side_effect=lambda r: r)
    uow.pipeline_runs.save_files = AsyncMock(return_value=None)
    uow.commit = AsyncMock()

    use_case = RecordPipelineRunUseCase(uow=uow)

    now = datetime.now(tz=UTC)
    file_record = PipelineRunFile(
        id="f1",
        pipeline_run_id="",
        file_path="/data/file1.csv",
        file_name="file1.csv",
        file_size_bytes=1024,
        mtime=now,
        hash_md5="abc123md5",
        status="PROCESSED",
        processed_at=now,
    )

    run = await use_case.execute(
        pipeline_id="p-123",
        pipeline_name="Ingest_Test",
        run_id="r-456",
        pipeline_type="ingestion",
        dag_run_id="manual__2026-08-27",
        status="success",
        started_at=now,
        finished_at=now,
        metrics={"row_count": 100},
        files=[file_record],
    )

    assert run.id == "r-456"
    assert run.status == PipelineRunStatus.SUCCESS
    assert run.metrics == {"row_count": 100}
    assert file_record.pipeline_run_id == "r-456"

    uow.pipeline_runs.save.assert_called_once()
    uow.pipeline_runs.save_files.assert_called_once_with([file_record])
    uow.commit.assert_called_once()
