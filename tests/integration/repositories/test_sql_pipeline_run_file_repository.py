from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.pipelines.pipeline import Pipeline
from app.domain.pipelines.pipeline_run import PipelineRun
from app.domain.pipelines.pipeline_run_file import PipelineRunFile
from app.domain.pipelines.pipeline_run_status import PipelineRunStatus
from app.domain.pipelines.pipeline_type import PipelineType
from app.domain.pipelines.schedule_config import ScheduleConfig, ScheduleMode
from app.domain.shared.value_objects import CronSchedule, EmailAddress
from app.infrastructure.persistence.repositories.sql_pipeline_repository import (
    SqlPipelineRepository,
)
from app.infrastructure.persistence.repositories.sql_pipeline_run_repository import (
    SqlPipelineRunRepository,
)


@pytest.mark.asyncio
async def test_save_and_find_pipeline_run_files(db_session: AsyncSession) -> None:
    pipeline_repo = SqlPipelineRepository(db_session)
    run_repo = SqlPipelineRunRepository(db_session)

    # 1. Create pipeline
    pipeline = Pipeline(
        id="pipe-1",
        name="test_pipeline",
        type=PipelineType.INGESTION,
        owner=EmailAddress("owner@co.com"),
        source_asset="asset-1",
        destination_asset="asset-2",
        schedule=ScheduleConfig(
            mode=ScheduleMode.CRON,
            cron_schedule=CronSchedule("0 0 * * *"),
        ),
    )
    await pipeline_repo.save(pipeline)

    # 2. Create pipeline run
    now = datetime.now(tz=UTC)
    run = PipelineRun(
        id="run-1",
        pipeline_id="pipe-1",
        pipeline_name="test_pipeline",
        pipeline_type="ingestion",
        dag_run_id="manual__1",
        status=PipelineRunStatus.SUCCESS,
        started_at=now,
        finished_at=now,
    )
    await run_repo.save(run)

    # 3. Create and save files
    f1 = PipelineRunFile(
        id="file-1",
        pipeline_run_id="run-1",
        file_path="/data/file1.csv",
        file_name="file1.csv",
        file_size_bytes=100,
        mtime=now,
        hash_md5="hash123",
        status="PROCESSED",
        processed_at=now,
    )
    f2 = PipelineRunFile(
        id="file-2",
        pipeline_run_id="run-1",
        file_path="/data/file2.csv",
        file_name="file2.csv",
        file_size_bytes=200,
        mtime=now,
        hash_md5="hash456",
        status="PENDING",
    )
    await run_repo.save_files([f1, f2])

    # 4. Find processed hashes
    hashes = await run_repo.find_processed_hashes_by_pipeline("pipe-1")
    assert "hash123" in hashes
    assert "hash456" not in hashes

    # 5. Update f2 to PROCESSED
    f2.mark_processed()
    await run_repo.save_files([f2])

    updated_hashes = await run_repo.find_processed_hashes_by_pipeline("pipe-1")
    assert "hash123" in updated_hashes
    assert "hash456" in updated_hashes
