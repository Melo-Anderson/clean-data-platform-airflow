from __future__ import annotations

import logging
import uuid

from app.application.discovery.discovery_provisioning_service import DiscoveryProvisioningService
from app.application.discovery.discovery_runner import DiscoveryRunnerFactory
from app.application.discovery.metadata_self_healing_service import MetadataSelfHealingService
from app.application.unit_of_work import UnitOfWork
from app.domain.assets.data_asset import DataAsset
from app.domain.discovery.discovery_run import DiscoveryRun
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.discovery.services.policy_tag_inferrer import PolicyTagInferrer
from app.domain.discovery.services.schema_differ import SchemaDiffer
from app.domain.discovery.services.schema_drift_service import SchemaDriftService
from app.domain.endpoints.endpoint import Endpoint
from app.domain.objects.data_object import DataObject

logger = logging.getLogger(__name__)


class RunDiscoveryUseCase:
    """Executes the metadata discovery process for a given DataAsset.

    Orchestrates:
    1. Initialization (load asset, endpoint, existing objects, create DiscoveryRun).
    2. Extraction (run discovery runner to get current SchemaSnapshots).
    3. Processing (auto-provision objects, compute drifts via SchemaDriftService,
       apply self-healing via MetadataSelfHealingService, complete the run).

    Does NOT contain drift computation or self-healing logic directly.
    """

    def __init__(
        self,
        uow: UnitOfWork,
        runner_factory: DiscoveryRunnerFactory,
        schema_differ: SchemaDiffer,
        tag_inferrer: PolicyTagInferrer,
    ) -> None:
        self._uow = uow
        self._runner_factory = runner_factory
        self._drift_service = SchemaDriftService(schema_differ, tag_inferrer)
        self._self_healing = MetadataSelfHealingService(uow)

    async def execute(self, asset_id: str, triggered_by: str) -> DiscoveryRun:
        run, endpoint, asset, objects = await self._initialize_run(asset_id, triggered_by)

        try:
            snapshots = await self._extract_snapshots(endpoint, asset)
            await self._process_discovery_results(asset_id, run, snapshots, objects)
            logger.info(
                "Discovery completed successfully | asset_id=%s | run_id=%s", asset_id, run.id
            )
            return run
        except Exception as e:
            logger.exception(
                "Discovery failed | asset_id=%s | triggered_by=%s", asset_id, triggered_by
            )
            await self._fail_run(run, str(e))
            raise

    async def _initialize_run(
        self, asset_id: str, triggered_by: str
    ) -> tuple[DiscoveryRun, Endpoint, DataAsset, list[DataObject]]:
        async with self._uow as uow:
            asset = await uow.assets.find_by_id(asset_id)
            self._validate_asset(asset, asset_id)
            assert asset is not None
            from typing import cast

            endpoint_id = cast(str, asset.endpoint_id)

            endpoint = await uow.endpoints.find_by_id(endpoint_id)
            if not endpoint:
                raise ValueError(f"Endpoint not found: {asset.endpoint_id}")

            objects = await uow.objects.find_by_asset_id(asset_id)
            run = DiscoveryRun(id=str(uuid.uuid4()), asset_id=asset_id, triggered_by=triggered_by)
            run.start()
            run = await uow.discovery_runs.save(run)
            await uow.commit()

            return run, endpoint, asset, objects

    def _validate_asset(self, asset: DataAsset | None, asset_id: str) -> None:
        if not asset:
            raise ValueError(f"Asset not found: {asset_id}")
        if not asset.endpoint_id:
            raise ValueError(f"Asset has no endpoint: {asset_id}")

    async def _extract_snapshots(
        self, endpoint: Endpoint, asset: DataAsset
    ) -> list[SchemaSnapshot]:
        runner = self._runner_factory.create(endpoint)
        scope_include = list(asset.discovery_scope.include)
        if not scope_include:
            scope_include = ["*"]
        scope_exclude = list(asset.discovery_scope.exclude)
        return await runner.run(asset.id, scope_include, scope_exclude, endpoint)

    async def _process_discovery_results(
        self,
        asset_id: str,
        run: DiscoveryRun,
        snapshots: list[SchemaSnapshot],
        objects: list[DataObject],
    ) -> None:
        async with self._uow as uow:
            provisioning_service = DiscoveryProvisioningService(uow)
            snapshots = await provisioning_service.provision_missing_objects(
                asset_id, snapshots, objects
            )

            baseline_run = await uow.discovery_runs.find_latest_by_asset_id(asset_id)
            prev_snapshots = {
                s.object_id: s for s in (baseline_run.snapshots if baseline_run else [])
            }

            # Delegate drift computation to the pure domain service
            events, suggestions = self._drift_service.compute_drifts_and_tags(
                prev_snapshots, snapshots
            )

            run.complete(
                snapshots=snapshots,
                drift_events=events,
                policy_tag_suggestions=suggestions,
                auto_generated_descriptions={},
                soft_failures=[],
            )

            # Delegate self-healing and approval persistence to the application service
            await self._self_healing.apply_self_healing_and_approvals(
                asset_id=asset_id,
                run_id=run.id,
                snapshots=snapshots,
                drift_events=events,
                prev_snapshots=prev_snapshots,
            )

            await uow.discovery_runs.save(run)
            await uow.commit()

    async def _fail_run(self, run: DiscoveryRun, error_message: str) -> None:
        async with self._uow as uow:
            run.fail(error_message)
            await uow.discovery_runs.save(run)
            await uow.commit()
