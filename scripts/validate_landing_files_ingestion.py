"""Validation script: Runs Discovery and Ingestion on the landing CSV and JSON datasets."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.application.discovery.run_discovery_use_case import RunDiscoveryUseCase
from app.application.pipelines.file_watermark_resolver import FileWatermarkResolver
from app.application.pipelines.register_pipeline import RegisterPipelineUseCase
from app.domain.assets.asset_state import AssetState
from app.domain.assets.data_asset import DataAsset
from app.domain.endpoints.endpoint import FileSystemEndpoint
from app.domain.shared.value_objects import (
    CredentialReference,
    CronSchedule,
    DiscoveryScope,
    EmailAddress,
)
from app.infrastructure.adapters.omnibeam.omnibeam_manifest_builder import (
    OmniBeamManifestBuilder,
)
from app.infrastructure.adapters.secrets.noop_secret_manager_adapter import (
    NoopSecretManagerAdapter,
)
from app.infrastructure.dag_generator.dag_generator import DagGenerator
from app.infrastructure.discovery.discovery_runner_factory import (
    DiscoveryRunnerFactoryImpl,
)
from app.infrastructure.persistence.database import get_session_factory
from app.infrastructure.persistence.sql_unit_of_work import SqlUnitOfWork
from app.infrastructure.yaml_generator.pipeline_yaml_generator import (
    PipelineYamlGenerator,
)


async def main() -> None:
    print("\n=== Validating Landing Files Ingestion (CSV & JSON) ===\n")

    landing_dir = Path("data/landing").resolve()
    dags_dir = Path("dags").resolve()
    output_dir = Path("logs/omnibeam_outputs").resolve()

    uow = SqlUnitOfWork(get_session_factory())

    # 1. Register Endpoint & Assets
    async with uow:
        existing_ep = await uow.endpoints.find_by_id("ep-landing-fs")
        if not existing_ep:
            endpoint = FileSystemEndpoint(
                id="ep-landing-fs",
                name="landing-filesystem",
                credential_ref=CredentialReference("vault/none"),
                root_path=str(landing_dir),
            )
            await uow.endpoints.save(endpoint)
        else:
            endpoint = existing_ep  # type: ignore[assignment]

        existing_csv_asset = await uow.assets.find_by_id("asset-transactions-csv")
        if not existing_csv_asset:
            asset_csv = DataAsset(
                id="asset-transactions-csv",
                name="transactions_csv_asset",
                description="Financial transactions CSV feed",
                owner=EmailAddress("finance-team@platform.local"),
                state=AssetState.ACTIVE,
                endpoint_id="ep-landing-fs",
                discovery_schedule=CronSchedule("0 0 * * *"),
                discovery_scope=DiscoveryScope(include=["*transactions*.csv:transactions"]),
            )
            await uow.assets.save(asset_csv)

        existing_json_asset = await uow.assets.find_by_id("asset-players-json")
        if not existing_json_asset:
            asset_json = DataAsset(
                id="asset-players-json",
                name="players_json_asset",
                description="Players gaming feed",
                owner=EmailAddress("analytics-team@platform.local"),
                state=AssetState.ACTIVE,
                endpoint_id="ep-landing-fs",
                discovery_schedule=CronSchedule("0 0 * * *"),
                discovery_scope=DiscoveryScope(include=["*players*.json:players"]),
            )
            await uow.assets.save(asset_json)
        await uow.commit()

    print("[+] FileSystem Endpoint and Assets registered successfully.")

    # 2. Run Discovery
    runner_factory = DiscoveryRunnerFactoryImpl(secret_manager=NoopSecretManagerAdapter())
    discovery_uc = RunDiscoveryUseCase(uow=uow, runner_factory=runner_factory)

    # 2.1 CSV Discovery
    csv_run = await discovery_uc.execute("asset-transactions-csv", triggered_by="cli_validator")
    csv_snapshot = csv_run.snapshots[0]
    csv_fields = [f.name for f in csv_snapshot.fields]
    print(
        f"[+] Discovery CSV: Object '{csv_snapshot.object_name}' found with {len(csv_fields)} fields: {', '.join(csv_fields)}"
    )

    # 2.2 JSON Discovery
    json_run = await discovery_uc.execute("asset-players-json", triggered_by="cli_validator")
    json_snapshot = json_run.snapshots[0]
    json_fields = [f.name for f in json_snapshot.fields]
    print(
        f"[+] Discovery JSON: Object '{json_snapshot.object_name}' found with {len(json_fields)} fields: {', '.join(json_fields)}"
    )

    # 3. Register Pipelines & Generate Airflow DAGs
    yaml_gen = PipelineYamlGenerator()
    dag_gen = DagGenerator()

    # 3.1 Transactions Pipeline
    async with uow:
        existing_txn = await uow.pipelines.find_by_name("ingest_transactions_landing")
        if not existing_txn:
            pipeline_uc = RegisterPipelineUseCase(
                uow=uow,
                dags_path=str(dags_dir),
                yaml_generator=yaml_gen,
                dag_generator=dag_gen,
            )
            pipe_txn = await pipeline_uc.execute(
                name="ingest_transactions_landing",
                pipeline_type="ingestion",
                owner_email="finance-team@platform.local",
                source_asset_id="asset-transactions-csv",
                cron_schedule="0 1 * * *",
                destination_asset="lakehouse_bronze",
                source_objects=[
                    {
                        "object_id": "asset-transactions-csv.transactions",
                        "load_strategy": "incremental",
                    }
                ],
                destination_objects=[{"object_name": "transactions"}],
                compute={"engine": "omnibeam", "staging_bucket": str(output_dir.as_posix())},
                quality_rules=[{"type": "not_null", "column": "transaction_id"}],
            )
        else:
            pipe_txn = existing_txn
            dag_code = dag_gen.generate(yaml_gen.generate(pipe_txn))
            (dags_dir / f"dag_p_{pipe_txn.name}.py").write_text(dag_code, encoding="utf-8")

    dag_txn_file = dags_dir / f"dag_p_{pipe_txn.name}.py"
    print(
        f"[+] Pipeline Transactions created/updated. DAG file: {dag_txn_file.name} (exists: {dag_txn_file.exists()})"
    )

    # 3.2 Players Pipeline
    async with uow:
        existing_players = await uow.pipelines.find_by_name("ingest_players_landing")
        if not existing_players:
            pipeline_uc = RegisterPipelineUseCase(
                uow=uow,
                dags_path=str(dags_dir),
                yaml_generator=yaml_gen,
                dag_generator=dag_gen,
            )
            pipe_players = await pipeline_uc.execute(
                name="ingest_players_landing",
                pipeline_type="ingestion",
                owner_email="analytics-team@platform.local",
                source_asset_id="asset-players-json",
                cron_schedule="*/30 * * * *",
                destination_asset="lakehouse_bronze",
                source_objects=[
                    {"object_id": "asset-players-json.players", "load_strategy": "incremental"}
                ],
                destination_objects=[{"object_name": "players"}],
                compute={"engine": "omnibeam", "staging_bucket": str(output_dir.as_posix())},
                quality_rules=[{"type": "not_null", "column": "player_id"}],
            )
        else:
            pipe_players = existing_players
            dag_code = dag_gen.generate(yaml_gen.generate(pipe_players))
            (dags_dir / f"dag_p_{pipe_players.name}.py").write_text(dag_code, encoding="utf-8")

    dag_players_file = dags_dir / f"dag_p_{pipe_players.name}.py"
    print(
        f"[+] Pipeline Players created/updated. DAG file: {dag_players_file.name} (exists: {dag_players_file.exists()})"
    )

    # 4. Incremental Watermark & Manifest Verification
    watermark_resolver = FileWatermarkResolver(uow=uow)
    manifest_builder = OmniBeamManifestBuilder()

    # 4.1 Resolve CSV files
    pending_csv = await watermark_resolver.resolve_pending_files(
        pipeline_id=pipe_txn.id,
        run_id="validation-run-001",
        endpoint=endpoint,
        scope_include=["*transactions*.csv"],
        scope_exclude=[],
    )
    manifest_csv = manifest_builder.build(
        pipeline_id=pipe_txn.id,
        run_id="validation-run-001",
        files=pending_csv,
        snapshot=csv_snapshot,
        output_path=str(output_dir / "bronze" / "transactions"),
        quarantine_path=str(output_dir / "dlq" / "transactions"),
    )
    print(
        f"[+] Watermark CSV: {len(pending_csv)} files resolved ({', '.join(f.file_name for f in pending_csv)}). Manifest Source Format: {manifest_csv.source.format}"
    )

    # 4.2 Resolve JSON files
    pending_json = await watermark_resolver.resolve_pending_files(
        pipeline_id=pipe_players.id,
        run_id="validation-run-002",
        endpoint=endpoint,
        scope_include=["*players*.json"],
        scope_exclude=[],
    )
    manifest_json = manifest_builder.build(
        pipeline_id=pipe_players.id,
        run_id="validation-run-002",
        files=pending_json,
        snapshot=json_snapshot,
        output_path=str(output_dir / "bronze" / "players"),
        quarantine_path=str(output_dir / "dlq" / "players"),
    )
    print(
        f"[+] Watermark JSON: {len(pending_json)} files resolved ({', '.join(f.file_name for f in pending_json)}). Manifest Source Format: {manifest_json.source.format}"
    )

    print("\n------------------------------------------------------------")
    print("LANDING FILES INGESTION VALIDATION: 100% SUCCESS")
    print("------------------------------------------------------------\n")


if __name__ == "__main__":
    asyncio.run(main())
