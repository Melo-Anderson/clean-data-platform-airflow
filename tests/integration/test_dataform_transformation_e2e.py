from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.application.pipelines.commands import RegisterPipelineCommand
from app.application.pipelines.register_pipeline import RegisterPipelineUseCase
from app.domain.assets.asset_state import AssetState
from app.domain.assets.data_asset import DataAsset
from app.domain.pipelines.pipeline_run import PipelineRun
from app.domain.pipelines.pipeline_run_status import PipelineRunStatus
from app.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress
from app.infrastructure.adapters.compute.dataform_compute_adapter import DataformComputeAdapter
from app.infrastructure.airflow_callbacks.transformation_callbacks import (
    evaluate_transformation_quality_gates,
    sync_transformation_catalog_metadata,
)
from app.infrastructure.dag_generator.dag_generator import DagGenerator
from app.infrastructure.persistence.database import get_session_factory
from app.infrastructure.persistence.sql_unit_of_work import SqlUnitOfWork
from app.infrastructure.yaml_generator.pipeline_yaml_generator import PipelineYamlGenerator


@pytest.mark.asyncio
async def test_dataform_transformation_full_lifecycle_e2e(tmp_path: Path) -> None:
    """
    E2E Test for Google Dataform Transformation Pipeline:
    1. Register Assets in PostgreSQL (Source bronze & Destination silver/gold).
    2. Register Transformation Pipeline & generate Airflow 3 Asset DAG.
    3. Execute DataformComputeAdapter with mock results and produce metrics.json.
    4. Evaluate Quality Gates against metrics.
    5. Sync Dataform compiled metadata & lineage into PostgreSQL.
    6. Record PipelineRun execution in database.
    """
    dags_dir = tmp_path / "dags"
    dags_dir.mkdir()
    dataform_out_dir = tmp_path / "dataform_outputs"
    dataform_out_dir.mkdir()

    uow = SqlUnitOfWork(get_session_factory())

    # 1. Register Source and Destination Assets
    async with uow:
        bronze_asset = DataAsset(
            id="asset-dataform-bronze",
            name="platform_dataform_bronze",
            description="Raw bronze gaming data",
            owner=EmailAddress("data@co.com"),
            state=AssetState.ACTIVE,
            endpoint_id=None,
            discovery_schedule=CronSchedule("0 2 * * *"),
            discovery_scope=DiscoveryScope(include=[]),
        )
        silver_asset = DataAsset(
            id="asset-dataform-silver",
            name="platform_dataform_silver",
            description="Silver cleaned gaming data",
            owner=EmailAddress("data@co.com"),
            state=AssetState.ACTIVE,
            endpoint_id=None,
            discovery_schedule=CronSchedule("0 3 * * *"),
            discovery_scope=DiscoveryScope(include=[]),
        )
        await uow.assets.save(bronze_asset)
        await uow.assets.save(silver_asset)
        await uow.commit()

    # 2. Register Pipeline & Generate DAG
    register_pipeline_uc = RegisterPipelineUseCase(
        uow=uow,
        dags_path=str(dags_dir),
        yaml_generator=PipelineYamlGenerator(),
        dag_generator=DagGenerator(),
    )

    pipeline = await register_pipeline_uc.execute(
        RegisterPipelineCommand(
            name="platform_dataform_pipeline",
            pipeline_type="transformation",
            owner_email="data@co.com",
            source_asset="asset-dataform-bronze",
            cron_schedule="",
            destination_asset="asset-dataform-silver",
            source_objects=[],
            destination_objects=[
                {"object_name": "dim_players"},
                {"object_name": "gold_fraud_alerts"},
            ],
            compute={
                "engine": "dataform",
                "project_dir": "dataform_project",
                "staging_bucket": str(dataform_out_dir),
            },
            quality_rules=[{"type": "not_null", "column": "player_id"}],
        )
    )

    assert pipeline.id is not None
    generated_dag_file = dags_dir / f"dag_p_{pipeline.name}.py"
    assert generated_dag_file.exists()
    dag_code = generated_dag_file.read_text(encoding="utf-8")
    assert "Asset(" in dag_code
    assert "run_transformations" in dag_code
    assert "engine='dataform'" in dag_code or 'engine="dataform"' in dag_code

    # 3. Execute Compute via DataformComputeAdapter
    def mock_dataform_executor(cmd: list[str], target_dir: Path) -> int:
        run_results = {
            "actions": [
                {"name": "slv_players", "type": "incremental", "status": "SUCCESS"},
                {"name": "dim_players", "type": "table", "status": "SUCCESS"},
                {"name": "assert_unique_player_sk", "type": "assertion", "status": "SUCCESS"},
            ],
            "elapsed_time": 3.2,
        }
        (target_dir / "dataform_results.json").write_text(json.dumps(run_results), encoding="utf-8")
        return 0

    compute_adapter = DataformComputeAdapter(
        project_dir="dataform_project",
        output_base_dir=str(dataform_out_dir),
        executor_fn=mock_dataform_executor,
    )
    job_id = compute_adapter.submit_job(
        pipeline_id=pipeline.id,
        pipeline_type="transformation",
        config={"select": "dim_players"},
    )
    job_res = compute_adapter.poll_job_status(job_id)
    assert job_res.status.value == "success"
    assert job_res.metrics_path is not None

    # 4. Evaluate Quality Gates
    q_result = evaluate_transformation_quality_gates(
        pipeline.id, job_res.metrics_path, engine="dataform"
    )
    assert q_result["quality_ok"] is True
    assert q_result["tests_passed"] == 1
    assert q_result["tests_failed"] == 0

    # 5. Sync Catalog Metadata & Lineage
    compilation_file = tmp_path / "compilation_result.json"
    fake_compilation = {
        "tables": [
            {
                "target": {
                    "database": "gcp-proj",
                    "schema": "platform_gold",
                    "name": "dim_players",
                },
                "type": "table",
                "actionDescriptor": {
                    "description": "Enriched players dimension",
                    "columns": [
                        {"path": ["player_sk"], "description": "Surrogate key"},
                        {"path": ["player_id"], "description": "Natural key"},
                    ],
                },
                "dependencyTargets": [
                    {
                        "database": "gcp-proj",
                        "schema": "platform_silver",
                        "name": "slv_players",
                    }
                ],
            }
        ],
        "declarations": [],
        "assertions": [],
    }
    compilation_file.write_text(json.dumps(fake_compilation), encoding="utf-8")

    sync_dict = sync_transformation_catalog_metadata(
        asset_id=silver_asset.id,
        manifest_path=str(compilation_file),
        engine="dataform",
    )
    assert sync_dict["synced"] is True
    assert sync_dict["objects_synced"] == 1
    assert sync_dict["elements_synced"] == 2

    # 6. Record PipelineRun in Database
    async with uow:
        run_record = PipelineRun(
            id="run-df-e2e-001",
            pipeline_id=pipeline.id,
            pipeline_name=pipeline.name,
            pipeline_type="transformation",
            dag_run_id="asset_triggered__df_e2e_1",
            status=PipelineRunStatus.SUCCESS,
            started_at=datetime.now(tz=UTC),
            finished_at=datetime.now(tz=UTC),
        )
        await uow.pipeline_runs.save(run_record)
        await uow.commit()

        saved_run = await uow.pipeline_runs.find_by_id("run-df-e2e-001")
        assert saved_run is not None
        assert saved_run.status == PipelineRunStatus.SUCCESS
