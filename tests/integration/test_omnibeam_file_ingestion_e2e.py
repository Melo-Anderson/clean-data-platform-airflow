from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.application.discovery.run_discovery_use_case import RunDiscoveryUseCase
from app.application.pipelines.file_watermark_resolver import FileWatermarkResolver
from app.domain.assets.asset_state import AssetState
from app.domain.assets.data_asset import DataAsset
from app.domain.endpoints.endpoint import FileSystemEndpoint
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
from app.infrastructure.discovery.discovery_runner_factory import (
    DiscoveryRunnerFactoryImpl,
)
from app.infrastructure.persistence.database import get_session_factory
from app.infrastructure.persistence.sql_unit_of_work import SqlUnitOfWork


@pytest.mark.asyncio
async def test_omnibeam_file_ingestion_full_flow(tmp_path: Path) -> None:
    # 1. Setup arquivos
    data_dir = tmp_path / "landing"
    data_dir.mkdir()
    f1 = data_dir / "pedidos_20260827_01.csv"
    f1.write_text("id,amount,status\n1,100.0,PAID\n2,200.0,PENDING\n", encoding="utf-8")

    uow = SqlUnitOfWork(get_session_factory())
    factory = DiscoveryRunnerFactoryImpl(secret_manager=NoopSecretManagerAdapter())
    discovery_uc = RunDiscoveryUseCase(uow=uow, runner_factory=factory)

    async with uow:
        ep = FileSystemEndpoint(
            id="ep-omni-fs",
            name="omni-fs",
            credential_ref=CredentialReference("vault/none"),
            root_path=str(data_dir),
        )
        await uow.endpoints.save(ep)

        asset = DataAsset(
            id="asset-omni-orders",
            name="omni-orders-asset",
            description="Orders folder",
            owner=EmailAddress("data@co.com"),
            state=AssetState.ACTIVE,
            endpoint_id="ep-omni-fs",
            discovery_schedule=CronSchedule("0 0 * * *"),
            discovery_scope=DiscoveryScope(include=["*pedidos*.csv:pedidos"]),
        )
        await uow.assets.save(asset)
        await uow.commit()

    # 2. Discovery
    disc_run = await discovery_uc.execute("asset-omni-orders", triggered_by="e2e_test")
    assert len(disc_run.snapshots) == 1
    snapshot = disc_run.snapshots[0]
    assert snapshot.object_name == "pedidos"

    # 3. Watermark Resolver - Primeira execução
    resolver = FileWatermarkResolver(uow=uow)
    pending_files = await resolver.resolve_pending_files(
        pipeline_id="pipe-omni-orders",
        run_id="run-001",
        endpoint=ep,
        scope_include=["*pedidos*.csv"],
        scope_exclude=[],
    )
    assert len(pending_files) == 1
    assert pending_files[0].file_name == "pedidos_20260827_01.csv"

    # 4. Manifest Builder
    builder = OmniBeamManifestBuilder()
    manifest = builder.build(
        pipeline_id="pipe-omni-orders",
        run_id="run-001",
        files=pending_files,
        snapshot=snapshot,
        output_path=str(tmp_path / "bronze" / "pedidos"),
        quarantine_path=str(tmp_path / "dlq" / "pedidos"),
        runner="direct",
    )

    manifest_dict = json.loads(manifest.to_json())
    assert manifest_dict["source"]["paths"] == [f1.resolve().as_posix()]
    assert manifest_dict["source"]["format"] == "csv"

    # 5. Compute Execution com mock runner local
    def fake_docker(cmd: list[str], output_dir: Path) -> int:
        (output_dir / "metrics.json").write_text(
            json.dumps({"row_count": 2, "null_count": 0}), encoding="utf-8"
        )
        (output_dir / "data.parquet").write_bytes(b"PAR1")
        return 0

    compute = OmniBeamComputeAdapter(
        output_base_dir=str(tmp_path / "output"), executor_fn=fake_docker
    )
    job_id = compute.submit_job(
        "pipe-omni-orders", "ingestion", {"manifest_json": manifest.to_json()}
    )
    result = compute.poll_job_status(job_id)
    assert result.status.value == "success"

    # 6. Marcar arquivo como processado e salvar no repositório
    pending_files[0].mark_processed()
    async with uow:
        # Criar pipeline_run primeiro para satisfazer a foreign key
        from datetime import UTC, datetime

        from app.domain.pipelines.pipeline_run import PipelineRun
        from app.domain.pipelines.pipeline_run_status import PipelineRunStatus

        p_run = PipelineRun(
            id="run-001",
            pipeline_id="pipe-omni-orders",
            pipeline_name="pipe-omni-orders",
            pipeline_type="ingestion",
            dag_run_id="manual__1",
            status=PipelineRunStatus.SUCCESS,
            started_at=datetime.now(tz=UTC),
            finished_at=datetime.now(tz=UTC),
        )
        await uow.pipeline_runs.save(p_run)
        await uow.pipeline_runs.save_files(pending_files)
        await uow.commit()

    # 7. Segunda execução de Watermark Resolver: arquivo já processado deve ser ignorado
    second_pending = await resolver.resolve_pending_files(
        pipeline_id="pipe-omni-orders",
        run_id="run-002",
        endpoint=ep,
        scope_include=["*pedidos*.csv"],
        scope_exclude=[],
    )
    assert len(second_pending) == 0


@pytest.mark.asyncio
async def test_omnibeam_json_ingestion_full_flow(tmp_path: Path) -> None:
    data_dir = tmp_path / "landing_json"
    data_dir.mkdir()
    f1 = data_dir / "events_20260827.json"
    f1.write_text(
        json.dumps(
            [
                {"event_id": "evt-1", "user_id": "usr-10", "action": "login"},
                {"event_id": "evt-2", "user_id": "usr-20", "action": "logout"},
            ]
        ),
        encoding="utf-8",
    )

    uow = SqlUnitOfWork(get_session_factory())
    factory = DiscoveryRunnerFactoryImpl(secret_manager=NoopSecretManagerAdapter())
    discovery_uc = RunDiscoveryUseCase(uow=uow, runner_factory=factory)

    async with uow:
        ep = FileSystemEndpoint(
            id="ep-omni-json-fs",
            name="omni-json-fs",
            credential_ref=CredentialReference("vault/none"),
            root_path=str(data_dir),
        )
        await uow.endpoints.save(ep)

        asset = DataAsset(
            id="asset-omni-events",
            name="omni-events-asset",
            description="Events JSON folder",
            owner=EmailAddress("data@co.com"),
            state=AssetState.ACTIVE,
            endpoint_id="ep-omni-json-fs",
            discovery_schedule=CronSchedule("0 0 * * *"),
            discovery_scope=DiscoveryScope(include=["*events*.json:events"]),
        )
        await uow.assets.save(asset)
        await uow.commit()

    disc_run = await discovery_uc.execute("asset-omni-events", triggered_by="e2e_test")
    assert len(disc_run.snapshots) == 1
    snapshot = disc_run.snapshots[0]
    assert snapshot.object_name == "events"

    resolver = FileWatermarkResolver(uow=uow)
    pending_files = await resolver.resolve_pending_files(
        pipeline_id="pipe-omni-events",
        run_id="run-001",
        endpoint=ep,
        scope_include=["*events*.json"],
        scope_exclude=[],
    )
    assert len(pending_files) == 1
    assert pending_files[0].file_name == "events_20260827.json"

    builder = OmniBeamManifestBuilder()
    manifest = builder.build(
        pipeline_id="pipe-omni-events",
        run_id="run-001",
        files=pending_files,
        snapshot=snapshot,
        output_path=str(tmp_path / "bronze" / "events"),
        quarantine_path=str(tmp_path / "dlq" / "events"),
        runner="direct",
    )

    manifest_dict = json.loads(manifest.to_json())
    assert manifest_dict["source"]["format"] == "jsonl"
    assert manifest_dict["source"]["paths"] == [f1.resolve().as_posix()]
