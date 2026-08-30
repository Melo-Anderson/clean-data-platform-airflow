from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.application.pipelines.register_pipeline import RegisterPipelineUseCase
from app.domain.assets.asset_state import AssetState
from app.domain.assets.data_asset import DataAsset
from app.domain.pipelines.pipeline_run import PipelineRun
from app.domain.pipelines.pipeline_run_status import PipelineRunStatus
from app.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress
from app.infrastructure.adapters.compute.dbt_compute_adapter import DbtComputeAdapter
from app.infrastructure.adapters.dbt.dbt_catalog_adapter import DbtCatalogAdapter
from app.infrastructure.adapters.dbt.dbt_manifest_parser import DbtManifestParser
from app.infrastructure.airflow_callbacks.transformation_callbacks import evaluate_dbt_quality_gates
from app.infrastructure.dag_generator.dag_generator import DagGenerator
from app.infrastructure.persistence.database import get_session_factory
from app.infrastructure.persistence.sql_unit_of_work import SqlUnitOfWork
from app.infrastructure.yaml_generator.pipeline_yaml_generator import PipelineYamlGenerator


@pytest.mark.asyncio
async def test_dbt_transformation_full_lifecycle_e2e(tmp_path: Path) -> None:
    """
    E2E Test for dbt Transformation Pipeline:
    1. Register Assets in PostgreSQL (Source bronze & Destination silver/gold).
    2. Register Transformation Pipeline & generate Airflow 3 Asset DAG.
    3. Execute DbtComputeAdapter with mock run_results and produce metrics.json.
    4. Evaluate Quality Gates against metrics.
    5. Sync dbt manifest metadata & lineage into PostgreSQL.
    6. Record PipelineRun execution in database.
    """
    dags_dir = tmp_path / "dags"
    dags_dir.mkdir()
    dbt_out_dir = tmp_path / "dbt_outputs"
    dbt_out_dir.mkdir()

    uow = SqlUnitOfWork(get_session_factory())

    # 1. Register Source and Destination Assets
    async with uow:
        bronze_asset = DataAsset(
            id="asset-platform-bronze",
            name="platform_bronze",
            description="Raw bronze gaming data",
            owner=EmailAddress("data@co.com"),
            state=AssetState.ACTIVE,
            endpoint_id=None,
            discovery_schedule=CronSchedule("0 2 * * *"),
            discovery_scope=DiscoveryScope(include=[]),
        )
        silver_asset = DataAsset(
            id="asset-platform-silver",
            name="platform_silver",
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
        name="platform_transformation_pipeline",
        pipeline_type="transformation",
        owner_email="data@co.com",
        source_asset_id="asset-platform-bronze",
        cron_schedule="",
        destination_asset="asset-platform-silver",
        source_objects=[],
        destination_objects=[{"object_name": "dim_players"}, {"object_name": "gold_fraud_alerts"}],
        compute={"engine": "dbt", "project_dir": "dbt_project", "staging_bucket": str(dbt_out_dir)},
        quality_rules=[{"type": "not_null", "column": "player_id"}],
    )

    assert pipeline.id is not None
    generated_dag_file = dags_dir / f"dag_p_{pipeline.name}.py"
    assert generated_dag_file.exists()
    dag_code = generated_dag_file.read_text(encoding="utf-8")
    assert "Asset(" in dag_code
    assert "run_dbt_transformations" in dag_code

    # 3. Execute Compute via DbtComputeAdapter
    def mock_dbt_executor(cmd: list[str], target_dir: Path) -> int:
        run_results = {
            "results": [
                {
                    "unique_id": "model.platform.dim_players",
                    "status": "success",
                    "execution_time": 1.5,
                },
                {
                    "unique_id": "model.platform.gold_fraud_alerts",
                    "status": "success",
                    "execution_time": 2.0,
                },
                {
                    "unique_id": "test.platform.unique_dim_players",
                    "status": "pass",
                    "execution_time": 0.3,
                },
            ],
            "elapsed_time": 3.8,
        }
        (target_dir / "run_results.json").write_text(json.dumps(run_results), encoding="utf-8")
        return 0

    compute_adapter = DbtComputeAdapter(
        project_dir="dbt_project",
        output_base_dir=str(dbt_out_dir),
        executor_fn=mock_dbt_executor,
    )
    job_id = compute_adapter.submit_job(
        pipeline_id=pipeline.id,
        pipeline_type="transformation",
        config={"select": "models/gold"},
    )
    job_res = compute_adapter.poll_job_status(job_id)
    assert job_res.status.value == "success"
    assert job_res.metrics_path is not None

    # 4. Evaluate Quality Gates
    q_result = evaluate_dbt_quality_gates(pipeline.id, job_res.metrics_path)
    assert q_result["quality_ok"] is True
    assert q_result["tests_passed"] == 1
    assert q_result["tests_failed"] == 0

    # 5. Sync Catalog Metadata & Lineage
    fake_manifest = {
        "nodes": {
            "model.platform.dim_players": {
                "unique_id": "model.platform.dim_players",
                "name": "dim_players",
                "resource_type": "model",
                "schema": "platform_gold",
                "description": "Enriched players dimension",
                "depends_on": {"nodes": ["model.platform.slv_players"]},
                "columns": {
                    "player_sk": {
                        "name": "player_sk",
                        "data_type": "STRING",
                        "description": "Surrogate key",
                    },
                    "player_id": {
                        "name": "player_id",
                        "data_type": "STRING",
                        "description": "Player natural key",
                    },
                },
            }
        },
        "sources": {},
    }
    manifest_parser = DbtManifestParser()
    manifest = manifest_parser.parse_dict(fake_manifest)
    catalog_adapter = DbtCatalogAdapter(uow=uow)
    sync_res = await catalog_adapter.sync_manifest(asset_id=silver_asset.id, manifest=manifest)
    assert sync_res.objects_synced == 1

    # 6. Record PipelineRun in Database
    async with uow:
        run_record = PipelineRun(
            id="run-dbt-e2e-001",
            pipeline_id=pipeline.id,
            pipeline_name=pipeline.name,
            pipeline_type="transformation",
            dag_run_id="asset_triggered__e2e_1",
            status=PipelineRunStatus.SUCCESS,
            started_at=datetime.now(tz=UTC),
            finished_at=datetime.now(tz=UTC),
        )
        await uow.pipeline_runs.save(run_record)
        await uow.commit()

        saved_run = await uow.pipeline_runs.find_by_id("run-dbt-e2e-001")
        assert saved_run is not None
        assert saved_run.status == PipelineRunStatus.SUCCESS
