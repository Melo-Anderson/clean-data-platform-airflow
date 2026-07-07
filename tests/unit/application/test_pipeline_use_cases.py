import pytest
import uuid
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch

from app.application.pipelines.register_pipeline import RegisterPipelineUseCase
from app.application.pipelines.trigger_pipeline_run import TriggerPipelineRunUseCase
from app.domain.pipelines.pipeline import Pipeline
from app.domain.pipelines.pipeline_run import PipelineRun
from app.domain.pipelines.pipeline_run_status import PipelineRunStatus
from app.domain.pipelines.pipeline_type import PipelineType
from app.domain.pipelines.schedule_config import ScheduleConfig
from app.domain.pipelines.schedule_mode import ScheduleMode
from app.domain.shared.value_objects import CronSchedule, EmailAddress


def make_uow():
    uow = MagicMock()
    uow.__aenter__ = AsyncMock(return_value=uow)
    uow.__aexit__ = AsyncMock(return_value=None)
    uow.commit = AsyncMock()
    return uow


@pytest.mark.asyncio
async def test_register_pipeline_saves_and_returns():
    uow = make_uow()
    saved_pipeline = Pipeline(
        id="pipe-001",
        name="ingest-e2e-asset",
        type=PipelineType.INGESTION,
        owner=EmailAddress("e2e@co.com"),
        schedule=ScheduleConfig(mode=ScheduleMode.CRON, cron_schedule=CronSchedule("0 0 * * *")),
        source_asset_id="asset-001",
        schema_version="1.0",
    )
    uow.pipelines.save = AsyncMock(return_value=saved_pipeline)

    use_case = RegisterPipelineUseCase(uow=uow)
    result = await use_case.execute(
        name="ingest-e2e-asset",
        pipeline_type="ingestion",
        owner_email="e2e@co.com",
        source_asset_id="asset-001",
        cron_schedule="0 0 * * *",
    )

    assert result.name == "ingest-e2e-asset"
    assert result.type == PipelineType.INGESTION
    uow.pipelines.save.assert_called_once()
    uow.commit.assert_called_once()


@pytest.mark.asyncio
async def test_trigger_run_creates_running_run():
    uow = make_uow()
    pipeline = Pipeline(
        id="pipe-001",
        name="ingest-e2e-asset",
        type=PipelineType.INGESTION,
        owner=EmailAddress("e2e@co.com"),
        schedule=ScheduleConfig(mode=ScheduleMode.CRON, cron_schedule=CronSchedule("0 0 * * *")),
        source_asset_id="asset-001",
        schema_version="1.0",
    )
    run = PipelineRun(
        id="run-001",
        pipeline_id="pipe-001",
        pipeline_name="ingest-e2e-asset",
        pipeline_type="ingestion",
        dag_run_id="e2e_test__2026-01-01T00:00:00",
        status=PipelineRunStatus.RUNNING,
        started_at=datetime.now(tz=UTC),
    )
    uow.pipelines.find_by_id = AsyncMock(return_value=pipeline)
    uow.pipeline_runs.save = AsyncMock(return_value=run)

    orchestrator = AsyncMock()
    orchestrator.trigger_dag = AsyncMock()

    use_case = TriggerPipelineRunUseCase(uow=uow, orchestrator=orchestrator)
    result = await use_case.execute(pipeline_id="pipe-001", triggered_by="e2e_test")

    assert result.status == PipelineRunStatus.RUNNING
    assert result.pipeline_id == "pipe-001"
    orchestrator.trigger_dag.assert_called_once()
