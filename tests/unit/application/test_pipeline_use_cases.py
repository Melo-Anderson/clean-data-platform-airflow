import pathlib
import unittest.mock
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.application.pipelines.register_pipeline import RegisterPipelineUseCase
from app.application.pipelines.trigger_pipeline_run import TriggerPipelineRunUseCase
from app.domain.pipelines.extraction_config import ExtractionConfig
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
    uow.assets.find_by_id = AsyncMock(return_value=None)
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
        source_asset="asset-001",
        schema_version="1.0",
    )
    uow.pipelines.save = AsyncMock(return_value=saved_pipeline)
    uow.pipelines.find_by_name = AsyncMock(return_value=None)

    use_case = RegisterPipelineUseCase(uow=uow)
    result = await use_case.execute(
        name="ingest-e2e-asset",
        pipeline_type="ingestion",
        owner_email="e2e@co.com",
        source_asset="asset-001",
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
    uow.assets.find_by_id = AsyncMock(return_value=make_asset("dst-1", "dst-1"))

    saved_pipeline = Pipeline(
        id="pipe-002",
        name="ingest-orders",
        type=PipelineType.INGESTION,
        owner=EmailAddress("eng@co.com"),
        schedule=ScheduleConfig(mode=ScheduleMode.CRON, cron_schedule=CronSchedule("0 6 * * *")),
        source_asset="src-1",
        destination_asset="dst-1",
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
        source_asset="src-1",
        cron_schedule="0 6 * * *",
        destination_asset="dst-1",
        destination_objects=[{"object_name": "orders_raw", "create_if_not_exists": True}],
    )

    dst_objects = [o for o in saved_objects if o.asset_id == "dst-1"]
    assert len(dst_objects) == 1
    assert dst_objects[0].name == "orders_raw"


from app.domain.assets.data_asset import DataAsset
from app.domain.shared.value_objects import DiscoveryScope


def make_asset(asset_id: str, name: str) -> DataAsset:
    return DataAsset(
        id=asset_id,
        name=name,
        description="Test asset",
        owner=EmailAddress("owner@co.com"),
        discovery_schedule=CronSchedule("0 6 * * *"),
        discovery_scope=DiscoveryScope(),
    )


@pytest.mark.asyncio
async def test_register_pipeline_calls_dwh_provisioner() -> None:
    uow = make_uow()
    uow.pipelines.find_by_name = AsyncMock(return_value=None)

    destination_asset = make_asset("dst-asset", "my-destination-asset")
    uow.assets.find_by_id = AsyncMock(return_value=destination_asset)

    saved_pipeline = Pipeline(
        id="pipe-dwh",
        name="ingest-customers",
        type=PipelineType.INGESTION,
        owner=EmailAddress("dwh@co.com"),
        schedule=ScheduleConfig(mode=ScheduleMode.CRON, cron_schedule=CronSchedule("0 0 * * *")),
        source_asset="src-asset",
        destination_asset="dst-asset",
        schema_version="1.0",
    )
    uow.pipelines.save = AsyncMock(return_value=saved_pipeline)
    uow.objects.find_by_asset_id = AsyncMock(return_value=[])
    uow.objects.save = AsyncMock()

    mock_dwh = AsyncMock()
    use_case = RegisterPipelineUseCase(uow=uow, dwh_provisioner=mock_dwh)

    await use_case.execute(
        name="ingest-customers",
        pipeline_type="ingestion",
        owner_email="dwh@co.com",
        source_asset="src-asset",
        cron_schedule="0 0 * * *",
        destination_asset="dst-asset",
        destination_objects=[{"object_name": "customers_table", "create_if_not_exists": True}],
    )

    mock_dwh.ensure_dataset_exists.assert_awaited_once_with(
        dataset_id="dst-asset",
        description="",
        labels={},
    )
    mock_dwh.ensure_table_exists.assert_awaited_once_with(
        dataset_id="dst-asset",
        table_id="customers_table",
        description="Auto-provisioned for pipeline 'ingest-customers'",
        labels={"managed_by": "clean_data_platform", "pipeline": "ingest-customers"},
        schema_fields=None,
    )


@pytest.mark.asyncio
async def test_trigger_run_creates_running_run(tmp_path) -> None:
    uow = make_uow()
    pipeline = Pipeline(
        id="pipe-001",
        name="ingest-e2e-asset",
        type=PipelineType.INGESTION,
        owner=EmailAddress("e2e@co.com"),
        schedule=ScheduleConfig(mode=ScheduleMode.CRON, cron_schedule=CronSchedule("0 0 * * *")),
        source_asset="asset-001",
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

    mock_yaml = MagicMock()
    mock_yaml.generate.return_value = "yaml"
    mock_dag = MagicMock()
    mock_dag.generate.return_value = "dag_code"

    use_case = TriggerPipelineRunUseCase(
        uow=uow,
        orchestrator=orchestrator,
        yaml_generator=mock_yaml,
        dag_generator=mock_dag,
        dags_path=str(tmp_path),
        telemetry=telemetry,
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
    use_case = TriggerPipelineRunUseCase(
        uow=uow, orchestrator=orchestrator, yaml_generator=MagicMock(), dag_generator=MagicMock()
    )

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
        source_asset="asset-002",
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

    mock_yaml = MagicMock()
    mock_yaml.generate.return_value = "yaml"
    mock_dag = MagicMock()
    mock_dag.generate.return_value = "dag_code"

    with (
        patch("pathlib.Path.mkdir"),
        patch("pathlib.Path.write_text"),
    ):
        use_case = TriggerPipelineRunUseCase(
            uow=uow,
            orchestrator=orchestrator,
            yaml_generator=mock_yaml,
            dag_generator=mock_dag,
            dags_path="/tmp/dags",
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
        source_asset="asset-003",
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

    mock_yaml = MagicMock()
    mock_yaml.generate.return_value = "yaml"
    mock_dag = MagicMock()
    mock_dag.generate.return_value = "# dag code"

    with (
        patch("pathlib.Path.mkdir") as mock_mkdir,
        patch("pathlib.Path.write_text") as mock_write,
    ):
        use_case = TriggerPipelineRunUseCase(
            uow=uow,
            orchestrator=orchestrator,
            yaml_generator=mock_yaml,
            dag_generator=mock_dag,
            dags_path="/tmp/dags",
        )
        await use_case.execute(pipeline_id="pipe-003", triggered_by="ci")

    mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
    mock_write.assert_called_once_with("# dag code", encoding="utf-8")


import re


@pytest.mark.asyncio
async def test_trigger_run_dag_run_id_is_airflow3_compatible() -> None:
    """dag_run_id deve conter apenas caracteres alfanuméricos, underscores e hífens (Airflow 3.0)."""
    uow = make_uow()
    pipeline = Pipeline(
        id="pipe-af3",
        name="airflow3-pipeline",
        type=PipelineType.INGESTION,
        owner=EmailAddress("eng@co.com"),
        schedule=ScheduleConfig(mode=ScheduleMode.CRON, cron_schedule=CronSchedule("0 6 * * *")),
        source_asset="asset-af3",
        schema_version="1.0",
    )
    captured_run = None

    async def capture_run(run):
        nonlocal captured_run
        captured_run = run
        return run

    uow.pipelines.find_by_id = AsyncMock(return_value=pipeline)
    uow.pipeline_runs.save = AsyncMock(side_effect=capture_run)

    orchestrator = AsyncMock()

    mock_yaml = MagicMock()
    mock_yaml.generate.return_value = "yaml"
    mock_dag = MagicMock()
    mock_dag.generate.return_value = "dag_code"

    with (
        patch("pathlib.Path.mkdir"),
        patch("pathlib.Path.write_text"),
    ):
        use_case = TriggerPipelineRunUseCase(
            uow=uow,
            orchestrator=orchestrator,
            yaml_generator=mock_yaml,
            dag_generator=mock_dag,
            dags_path="/tmp/dags",
        )
        await use_case.execute(pipeline_id="pipe-af3", triggered_by="ci")

    assert captured_run is not None
    dag_run_id = captured_run.dag_run_id
    # Airflow 3.0 regex: ^[a-zA-Z0-9._-]+$  (no colons, no plus signs)
    assert re.match(r"^[a-zA-Z0-9._\-]+$", dag_run_id), (
        f"dag_run_id '{dag_run_id}' contains characters not allowed by Airflow 3.0"
    )
    # Must start with the triggered_by prefix
    assert dag_run_id.startswith("ci__")


@pytest.mark.asyncio
async def test_register_pipeline_uses_asset_name_as_dataset_id() -> None:
    """ensure_table_exists deve receber asset.name como dataset_id, não o UUID."""
    uow = make_uow()
    uow.pipelines.find_by_name = AsyncMock(return_value=None)

    destination_asset = make_asset("dst-uuid-1234", "e2e-postgres-asset")
    uow.assets.find_by_id = AsyncMock(return_value=destination_asset)

    saved_pipeline = Pipeline(
        id="pipe-name-test",
        name="ingest-for-name-test",
        type=PipelineType.INGESTION,
        owner=EmailAddress("eng@co.com"),
        schedule=ScheduleConfig(mode=ScheduleMode.CRON, cron_schedule=CronSchedule("0 0 * * *")),
        source_asset="src-asset",
        destination_asset="dst-uuid-1234",
        schema_version="1.0",
    )
    uow.pipelines.save = AsyncMock(return_value=saved_pipeline)
    uow.objects.find_by_asset_id = AsyncMock(return_value=[])
    uow.objects.save = AsyncMock()

    mock_dwh = AsyncMock()
    use_case = RegisterPipelineUseCase(uow=uow, dwh_provisioner=mock_dwh)

    await use_case.execute(
        name="ingest-for-name-test",
        pipeline_type="ingestion",
        owner_email="eng@co.com",
        source_asset="src-asset",
        cron_schedule="0 0 * * *",
        destination_asset="dst-uuid-1234",
        destination_objects=[{"object_name": "orders_stg", "create_if_not_exists": True}],
    )

    # ensure_dataset_exists deve ter sido chamado com o dataset de destino exato
    mock_dwh.ensure_dataset_exists.assert_awaited_once_with(
        dataset_id="dst-uuid-1234",
        description="",
        labels={},
    )
    # ensure_table_exists também deve usar o dataset de destino exato
    mock_dwh.ensure_table_exists.assert_awaited_once_with(
        dataset_id="dst-uuid-1234",
        table_id="orders_stg",
        description="Auto-provisioned for pipeline 'ingest-for-name-test'",
        labels={"managed_by": "clean_data_platform", "pipeline": "ingest-for-name-test"},
        schema_fields=None,
    )


@pytest.mark.asyncio
async def test_register_pipeline_falls_back_to_id_when_asset_not_found() -> None:
    """Se o asset de destino não existe, usa destination_asset_id como fallback."""
    uow = make_uow()
    uow.pipelines.find_by_name = AsyncMock(return_value=None)
    uow.assets.find_by_id = AsyncMock(return_value=None)  # asset não encontrado

    saved_pipeline = Pipeline(
        id="pipe-fallback",
        name="ingest-fallback",
        type=PipelineType.INGESTION,
        owner=EmailAddress("eng@co.com"),
        schedule=ScheduleConfig(mode=ScheduleMode.CRON, cron_schedule=CronSchedule("0 0 * * *")),
        source_asset="src-asset",
        destination_asset="dst-uuid-fallback",
        schema_version="1.0",
    )
    uow.pipelines.save = AsyncMock(return_value=saved_pipeline)
    uow.objects.find_by_asset_id = AsyncMock(return_value=[])
    uow.objects.save = AsyncMock()

    mock_dwh = AsyncMock()
    use_case = RegisterPipelineUseCase(uow=uow, dwh_provisioner=mock_dwh)

    await use_case.execute(
        name="ingest-fallback",
        pipeline_type="ingestion",
        owner_email="eng@co.com",
        source_asset="src-asset",
        cron_schedule="0 0 * * *",
        destination_asset="dst-uuid-fallback",
        destination_objects=[{"object_name": "tbl_stg", "create_if_not_exists": True}],
    )

    # Fallback: usa o destination_asset quando asset não é encontrado
    mock_dwh.ensure_dataset_exists.assert_awaited_once_with(
        dataset_id="dst-uuid-fallback",
        description="",
        labels={},
    )


@pytest.mark.asyncio
async def test_register_pipeline_maps_source_objects_and_writes_dag(tmp_path: pathlib.Path) -> None:
    """RegisterPipelineUseCase deve mapear source_objects e gravar o arquivo DAG."""
    uow = make_uow()
    saved_pipeline = Pipeline(
        id="pipe-010",
        name="ingest_orders",
        type=PipelineType.INGESTION,
        owner=EmailAddress("eng@co.com"),
        schedule=ScheduleConfig(mode=ScheduleMode.CRON, cron_schedule=CronSchedule("0 * * * *")),
        source_asset="asset-001",
        source_objects=[
            ExtractionConfig(
                object_id="demo_orders",
                extraction_query="SELECT id FROM demo_orders",
            )
        ],
        schema_version="1.0",
    )
    uow.pipelines.save = AsyncMock(return_value=saved_pipeline)
    uow.pipelines.find_by_name = AsyncMock(return_value=None)

    use_case = RegisterPipelineUseCase(uow=uow, dags_path=str(tmp_path))
    result = await use_case.execute(
        name="ingest_orders",
        pipeline_type="ingestion",
        owner_email="eng@co.com",
        source_asset="asset-001",
        cron_schedule="0 * * * *",
        source_objects=[
            {"object_id": "demo_orders", "extraction_query": "SELECT id FROM demo_orders"}
        ],
    )

    assert result.name == "ingest_orders"
    dag_file = tmp_path / "dag_p_ingest_orders.py"
    assert dag_file.exists(), "DAG file must be written by RegisterPipelineUseCase"
    assert "ingest_orders" in dag_file.read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_register_pipeline_without_source_objects_still_writes_dag(
    tmp_path: pathlib.Path,
) -> None:
    """Registro sem source_objects tambem deve gerar o arquivo DAG."""
    uow = make_uow()
    saved_pipeline = Pipeline(
        id="pipe-011",
        name="ingest_customers",
        type=PipelineType.INGESTION,
        owner=EmailAddress("eng@co.com"),
        schedule=ScheduleConfig(mode=ScheduleMode.CRON, cron_schedule=CronSchedule("0 0 * * *")),
        source_asset="asset-002",
        schema_version="1.0",
    )
    uow.pipelines.save = AsyncMock(return_value=saved_pipeline)
    uow.pipelines.find_by_name = AsyncMock(return_value=None)

    use_case = RegisterPipelineUseCase(uow=uow, dags_path=str(tmp_path))
    await use_case.execute(
        name="ingest_customers",
        pipeline_type="ingestion",
        owner_email="eng@co.com",
        source_asset="asset-002",
        cron_schedule="0 0 * * *",
    )

    assert (tmp_path / "dag_p_ingest_customers.py").exists()
