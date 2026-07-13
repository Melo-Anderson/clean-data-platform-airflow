import unittest.mock
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

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
async def test_register_pipeline_saves_and_returns() -> None:
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
    uow.pipelines.find_by_name = AsyncMock(return_value=None)

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
async def test_register_pipeline_creates_destination_objects() -> None:
    uow = make_uow()

    # Mock find_by_name to return None (no existing pipeline)
    uow.pipelines.find_by_name = AsyncMock(return_value=None)

    saved_pipeline = Pipeline(
        id="pipe-002",
        name="ingest-orders",
        type=PipelineType.INGESTION,
        owner=EmailAddress("eng@co.com"),
        schedule=ScheduleConfig(mode=ScheduleMode.CRON, cron_schedule=CronSchedule("0 6 * * *")),
        source_asset_id="src-1",
        destination_asset_id="dst-1",
        schema_version="1.0",
    )
    uow.pipelines.save = AsyncMock(return_value=saved_pipeline)

    # Track saved objects
    saved_objects = []

    async def mock_save_obj(obj):
        saved_objects.append(obj)
        return obj

    uow.objects.save.side_effect = mock_save_obj
    uow.objects.find_by_asset_id = AsyncMock(return_value=[])

    use_case = RegisterPipelineUseCase(uow=uow)
    await use_case.execute(
        name="ingest-orders",
        pipeline_type="ingestion",
        owner_email="eng@co.com",
        source_asset_id="src-1",
        cron_schedule="0 6 * * *",
        destination_asset_id="dst-1",
        destination_objects=[{"name": "orders_raw", "create_if_not_exists": True}],
    )

    dst_objects = [o for o in saved_objects if o.asset_id == "dst-1"]
    assert len(dst_objects) == 1
    assert dst_objects[0].name == "orders_raw"


@pytest.mark.asyncio
async def test_trigger_run_creates_running_run(tmp_path) -> None:
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

    telemetry = MagicMock()

    use_case = TriggerPipelineRunUseCase(
        uow=uow, orchestrator=orchestrator, dags_path=str(tmp_path), telemetry=telemetry
    )
    result = await use_case.execute(pipeline_id="pipe-001", triggered_by="e2e_test")

    assert result.status == PipelineRunStatus.RUNNING
    assert result.pipeline_id == "pipe-001"
    orchestrator.trigger_dag.assert_called_once()
    telemetry.record_event.assert_called_once_with(
        "platform.pipeline.triggered",
        {"pipeline_id": "pipe-001", "run_id": "run-001", "pipeline_name": "ingest-e2e-asset"},
    )


@pytest.mark.asyncio
async def test_trigger_run_raises_when_pipeline_not_found() -> None:
    """execute() deve levantar ValueError quando o pipeline não existe."""
    uow = make_uow()
    uow.pipelines.find_by_id = AsyncMock(return_value=None)

    orchestrator = AsyncMock()
    use_case = TriggerPipelineRunUseCase(uow=uow, orchestrator=orchestrator)

    with pytest.raises(ValueError, match="Pipeline not found: unknown-id"):
        await use_case.execute(pipeline_id="unknown-id", triggered_by="ci")

    orchestrator.trigger_dag.assert_not_called()


@pytest.mark.asyncio
async def test_trigger_run_calls_trigger_dag_with_correct_args() -> None:
    """trigger_dag deve ser chamado com pipeline_id, run_id, dag_run_id e pipeline_name corretos."""
    uow = make_uow()
    pipeline = Pipeline(
        id="pipe-002",
        name="my-pipeline",
        type=PipelineType.INGESTION,
        owner=EmailAddress("eng@co.com"),
        schedule=ScheduleConfig(mode=ScheduleMode.CRON, cron_schedule=CronSchedule("0 6 * * *")),
        source_asset_id="asset-002",
        schema_version="1.0",
    )
    run = PipelineRun(
        id="run-002",
        pipeline_id="pipe-002",
        pipeline_name="my-pipeline",
        pipeline_type="ingestion",
        dag_run_id="ci__2026-01-01T00:00:00",
        status=PipelineRunStatus.RUNNING,
        started_at=datetime.now(tz=UTC),
    )
    uow.pipelines.find_by_id = AsyncMock(return_value=pipeline)
    uow.pipeline_runs.save = AsyncMock(return_value=run)

    orchestrator = AsyncMock()

    with (
        patch("app.application.pipelines.trigger_pipeline_run.PipelineYamlGenerator") as MockYaml,
        patch("app.application.pipelines.trigger_pipeline_run.DagGenerator") as MockDag,
        patch("pathlib.Path.mkdir"),
        patch("pathlib.Path.write_text"),
    ):
        MockYaml.return_value.generate.return_value = "yaml"
        MockDag.return_value.generate.return_value = "dag_code"

        use_case = TriggerPipelineRunUseCase(
            uow=uow, orchestrator=orchestrator, dags_path="/tmp/dags"
        )
        await use_case.execute(pipeline_id="pipe-002", triggered_by="ci")

    orchestrator.trigger_dag.assert_called_once_with(
        pipeline_id="pipe-002",
        run_id="run-002",
        dag_run_id=unittest.mock.ANY,  # valor gerado dinamicamente com datetime
        pipeline_name="my-pipeline",
    )


@pytest.mark.asyncio
async def test_trigger_run_writes_dag_file() -> None:
    """O arquivo DAG deve ser escrito em dags_path/<pipeline_name>.py."""
    uow = make_uow()
    pipeline = Pipeline(
        id="pipe-003",
        name="orders-ingest",
        type=PipelineType.INGESTION,
        owner=EmailAddress("eng@co.com"),
        schedule=ScheduleConfig(mode=ScheduleMode.CRON, cron_schedule=CronSchedule("0 8 * * *")),
        source_asset_id="asset-003",
        schema_version="1.0",
    )
    run = PipelineRun(
        id="run-003",
        pipeline_id="pipe-003",
        pipeline_name="orders-ingest",
        pipeline_type="ingestion",
        dag_run_id="ci__2026-01-02T00:00:00",
        status=PipelineRunStatus.RUNNING,
        started_at=datetime.now(tz=UTC),
    )
    uow.pipelines.find_by_id = AsyncMock(return_value=pipeline)
    uow.pipeline_runs.save = AsyncMock(return_value=run)

    orchestrator = AsyncMock()

    with (
        patch("app.application.pipelines.trigger_pipeline_run.PipelineYamlGenerator") as MockYaml,
        patch("app.application.pipelines.trigger_pipeline_run.DagGenerator") as MockDag,
        patch("pathlib.Path.mkdir") as mock_mkdir,
        patch("pathlib.Path.write_text") as mock_write,
    ):
        MockYaml.return_value.generate.return_value = "yaml"
        MockDag.return_value.generate.return_value = "# dag code"

        use_case = TriggerPipelineRunUseCase(
            uow=uow, orchestrator=orchestrator, dags_path="/tmp/dags"
        )
        await use_case.execute(pipeline_id="pipe-003", triggered_by="ci")

    mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
    mock_write.assert_called_once_with("# dag code", encoding="utf-8")
