from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from app.application.pipelines.record_pipeline_run_use_case import RecordPipelineRunUseCase
from app.domain.pipelines.pipeline_run import PipelineRun
from app.domain.pipelines.watermark import Watermark, WatermarkType


@pytest.mark.asyncio
async def test_success_run_projects_metrics_watermark() -> None:
    saved_run: PipelineRun | None = None

    mock_runs_repo = AsyncMock()

    async def _save(run: PipelineRun) -> PipelineRun:
        nonlocal saved_run
        saved_run = run
        return run

    mock_runs_repo.save = AsyncMock(side_effect=_save)
    mock_runs_repo.save_files = AsyncMock()

    mock_uow = AsyncMock()
    mock_uow.pipeline_runs = mock_runs_repo

    use_case = RecordPipelineRunUseCase(uow=mock_uow)

    await use_case.execute(
        pipeline_id="p-1",
        pipeline_name="orders_pipe",
        started_at=datetime.now(tz=UTC),
        status="success",
        metrics={
            "row_count": 100,
            "watermark": {
                "column": "id",
                "type": "integer",
                "value": "10500",
            },
        },
    )

    assert saved_run is not None
    assert saved_run.watermark is not None
    assert saved_run.watermark == Watermark(value="10500", type=WatermarkType.INTEGER)


@pytest.mark.asyncio
async def test_failed_and_quality_failed_runs_do_not_project_watermark() -> None:
    saved_run: PipelineRun | None = None
    mock_runs_repo = AsyncMock()

    async def _save(run: PipelineRun) -> PipelineRun:
        nonlocal saved_run
        saved_run = run
        return run

    mock_runs_repo.save = AsyncMock(side_effect=_save)
    mock_uow = AsyncMock()
    mock_uow.pipeline_runs = mock_runs_repo

    use_case = RecordPipelineRunUseCase(uow=mock_uow)

    # Failed run
    await use_case.execute(
        pipeline_id="p-1",
        pipeline_name="orders_pipe",
        started_at=datetime.now(tz=UTC),
        status="failed",
        metrics={
            "watermark": {
                "column": "id",
                "type": "integer",
                "value": "10500",
            },
        },
    )
    assert saved_run is not None
    assert saved_run.watermark is None

    # Quality failed run
    await use_case.execute(
        pipeline_id="p-1",
        pipeline_name="orders_pipe",
        started_at=datetime.now(tz=UTC),
        status="quality_failed",
        metrics={
            "watermark": {
                "column": "id",
                "type": "integer",
                "value": "10500",
            },
        },
    )
    assert saved_run.watermark is None


@pytest.mark.asyncio
async def test_null_value_watermark_does_not_project() -> None:
    saved_run: PipelineRun | None = None
    mock_runs_repo = AsyncMock()

    async def _save(run: PipelineRun) -> PipelineRun:
        nonlocal saved_run
        saved_run = run
        return run

    mock_runs_repo.save = AsyncMock(side_effect=_save)
    mock_uow = AsyncMock()
    mock_uow.pipeline_runs = mock_runs_repo

    use_case = RecordPipelineRunUseCase(uow=mock_uow)

    await use_case.execute(
        pipeline_id="p-1",
        pipeline_name="orders_pipe",
        started_at=datetime.now(tz=UTC),
        status="success",
        metrics={
            "watermark": {
                "column": "id",
                "type": "integer",
                "value": None,
            },
        },
    )
    assert saved_run is not None
    assert saved_run.watermark is None
