from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.application.discovery.run_discovery_use_case import RunDiscoveryUseCase
from app.application.pipelines.file_watermark_resolver import FileWatermarkResolver
from app.application.pipelines.register_pipeline import RegisterPipelineUseCase
from app.domain.assets.asset_state import AssetState
from app.domain.assets.data_asset import DataAsset
from app.domain.endpoints.endpoint import FileSystemEndpoint
from app.domain.pipelines.pipeline_run import PipelineRun
from app.domain.pipelines.pipeline_run_status import PipelineRunStatus
from app.domain.shared.value_objects import (
    CredentialReference,
    CronSchedule,
    DiscoveryScope,
    EmailAddress,
)
from app.infrastructure.adapters.compute.omnibeam_compute_adapter import (
    OmniBeamComputeAdapter,
)
from app.infrastructure.adapters.omnibeam.omnibeam_manifest_builder import (
    OmniBeamManifestBuilder,
)
from app.infrastructure.adapters.secrets.noop_secret_manager_adapter import (
    NoopSecretManagerAdapter,
)
from app.infrastructure.airflow_callbacks.ingestion_callbacks import (
    load_to_data_warehouse,
    post_load_validation,
)
from app.infrastructure.airflow_callbacks.shared_callbacks import quality_gate
from app.infrastructure.dag_generator.dag_generator import DagGenerator
from app.infrastructure.discovery.discovery_runner_factory import (
    DiscoveryRunnerFactoryImpl,
)
from app.infrastructure.persistence.database import get_session_factory
from app.infrastructure.persistence.sql_unit_of_work import SqlUnitOfWork
from app.infrastructure.yaml_generator.pipeline_yaml_generator import (
    PipelineYamlGenerator,
)


@pytest.mark.asyncio
async def test_full_platform_lifecycle_from_asset_to_execution_e2e(tmp_path: Path) -> None:
    """
    End-to-End Test:
    1. Register Endpoint (FileSystem)
    2. Register Asset (DataAsset with discovery scope)
    3. Run Discovery (Schema inference & snapshot)
    4. Register Pipeline (Generates pipeline YAML & Airflow DAG)
    5. Incremental Watermark Resolution (Pending files)
    6. OmniBeam Compute Execution (Generates parquet & metrics)
    7. Quality Gates Evaluation
    8. DWH Loading & Post-load Verification
    9. Watermark Audit Marking (PROCESSED)
    """
    # ── 0. Prepare local directories and test file ──────────────────────────
    landing_dir = tmp_path / "landing"
    landing_dir.mkdir()
    orders_file = landing_dir / "orders_20260827_01.csv"
    orders_file.write_text(
        "id,customer_id,amount,status\n1,cust_1,150.50,PAID\n2,cust_2,299.90,PAID\n",
        encoding="utf-8",
    )

    dags_dir = tmp_path / "dags"
    dags_dir.mkdir()

    output_dir = tmp_path / "omnibeam_outputs"
    output_dir.mkdir()

    uow = SqlUnitOfWork(get_session_factory())

    # ── 1 & 2. Register Endpoint and Asset ──────────────────────────────────
    async with uow:
        endpoint = FileSystemEndpoint(
            id="ep-local-fs",
            name="local-filesystem",
            credential_ref=CredentialReference("vault/none"),
            root_path=str(landing_dir),
        )
        await uow.endpoints.save(endpoint)

        asset = DataAsset(
            id="asset-orders-source",
            name="orders_source_asset",
            description="Source directory for sales orders",
            owner=EmailAddress("data-team@co.com"),
            state=AssetState.ACTIVE,
            endpoint_id="ep-local-fs",
            discovery_schedule=CronSchedule("0 0 * * *"),
            discovery_scope=DiscoveryScope(include=["*orders*.csv:orders"]),
        )
        await uow.assets.save(asset)
        await uow.commit()

    # ── 3. Run Discovery ────────────────────────────────────────────────────
    factory = DiscoveryRunnerFactoryImpl(secret_manager=NoopSecretManagerAdapter())
    discovery_uc = RunDiscoveryUseCase(uow=uow, runner_factory=factory)
    discovery_run = await discovery_uc.execute(
        "asset-orders-source", triggered_by="e2e_platform_test"
    )

    assert len(discovery_run.snapshots) == 1
    snapshot = discovery_run.snapshots[0]
    assert snapshot.object_name == "orders"
    field_names = [f.name for f in snapshot.fields]
    assert "id" in field_names
    assert "amount" in field_names

    # ── 4. Register Pipeline & Generate DAG ─────────────────────────────────
    register_pipeline_uc = RegisterPipelineUseCase(
        uow=uow,
        dags_path=str(dags_dir),
        yaml_generator=PipelineYamlGenerator(),
        dag_generator=DagGenerator(),
    )

    pipeline = await register_pipeline_uc.execute(
        name="ingest_orders_omnibeam",
        pipeline_type="ingestion",
        owner_email="data-team@co.com",
        source_asset_id="asset-orders-source",
        cron_schedule="0 2 * * *",
        destination_asset="asset-lakehouse-orders",
        source_objects=[
            {"object_id": "asset-orders-source.orders", "load_strategy": "incremental"}
        ],
        destination_objects=[{"object_name": "orders"}],
        compute={"engine": "omnibeam", "staging_bucket": str(output_dir)},
        quality_rules=[{"type": "not_null", "column": "id"}],
    )

    assert pipeline.id is not None
    generated_dag_file = dags_dir / f"dag_p_{pipeline.name}.py"
    assert generated_dag_file.exists()
    dag_code = generated_dag_file.read_text(encoding="utf-8")
    assert "submit_compute_job" in dag_code

    # ── 5. Incremental Watermark Resolution ─────────────────────────────────
    watermark_resolver = FileWatermarkResolver(uow=uow)
    pending_files = await watermark_resolver.resolve_pending_files(
        pipeline_id=pipeline.id,
        run_id="run-e2e-001",
        endpoint=endpoint,
        scope_include=["*orders*.csv"],
        scope_exclude=[],
    )
    assert len(pending_files) == 1
    assert pending_files[0].file_name == "orders_20260827_01.csv"

    # ── 6. Build Manifest & Execute Compute (OmniBeam) ──────────────────────
    manifest_builder = OmniBeamManifestBuilder()
    manifest = manifest_builder.build(
        pipeline_id=pipeline.id,
        run_id="run-e2e-001",
        files=pending_files,
        snapshot=snapshot,
        output_path=str(output_dir / "bronze" / "orders"),
        quarantine_path=str(output_dir / "dlq" / "orders"),
        runner="direct",
    )

    def mock_omnibeam_binary_exec(cmd: list[str], job_output_dir: Path) -> int:
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.Table.from_arrays(
            [
                pa.array([1, 2]),
                pa.array(["cust_1", "cust_2"]),
                pa.array([150.50, 299.90]),
                pa.array(["PAID", "PAID"]),
            ],
            names=["id", "customer_id", "amount", "status"],
        )
        pq.write_table(table, job_output_dir / "data.parquet")
        (job_output_dir / "metrics.json").write_text(
            json.dumps({"row_count": 2, "null_count_id": 0, "checksum": "abc123md5"}),
            encoding="utf-8",
        )
        return 0

    fake_bin = tmp_path / "bin" / "omnibeam-pipeline"
    fake_bin.parent.mkdir()
    fake_bin.write_text("#!/bin/sh\nexit 0", encoding="utf-8")

    compute_adapter = OmniBeamComputeAdapter(
        output_base_dir=str(output_dir),
        binary_path=str(fake_bin),
        executor_fn=mock_omnibeam_binary_exec,
    )
    job_id = compute_adapter.submit_job(
        pipeline.id, "ingestion", {"manifest_json": manifest.to_json()}
    )
    job_res = compute_adapter.poll_job_status(job_id)
    assert job_res.status.value == "success"

    metrics_data = json.loads(Path(job_res.metrics_path).read_text(encoding="utf-8"))
    q_result = quality_gate(
        pipeline_id=pipeline.id,
        metrics=metrics_data,
        quality_rules=[{"type": "not_null", "column": "id"}],
    )
    assert q_result["quality_ok"] is True

    load_result = load_to_data_warehouse(
        pipeline_id=pipeline.id,
        destination_object_ids=["orders"],
        staging_path=job_res.output_path,
        schema_path=None,
        engine_type="noop",
        connection_metadata={"dataset": "bronze", "table": "orders"},
    )

    assert load_result["loaded"] is True

    post_valid = post_load_validation(
        pipeline_id=pipeline.id,
        expected_rows=2,
        actual_rows=load_result["rows_loaded"],
        source_checksum=None,
        destination_checksum=None,
    )
    assert post_valid["validation_ok"] is True

    # ── 9. Audit Marking & Watermark Verification ───────────────────────────
    pending_files[0].mark_processed()
    async with uow:
        run_record = PipelineRun(
            id="run-e2e-001",
            pipeline_id=pipeline.id,
            pipeline_name=pipeline.name,
            pipeline_type="ingestion",
            dag_run_id="manual__e2e_1",
            status=PipelineRunStatus.SUCCESS,
            started_at=datetime.now(tz=UTC),
            finished_at=datetime.now(tz=UTC),
        )
        await uow.pipeline_runs.save(run_record)
        await uow.pipeline_runs.save_files(pending_files)
        await uow.commit()

    # Verify second resolution skips the ingested file
    subsequent_pending = await watermark_resolver.resolve_pending_files(
        pipeline_id=pipeline.id,
        run_id="run-e2e-002",
        endpoint=endpoint,
        scope_include=["*orders*.csv"],
        scope_exclude=[],
    )
    assert len(subsequent_pending) == 0
