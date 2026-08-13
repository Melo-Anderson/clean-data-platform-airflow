from __future__ import annotations

import logging
import uuid
from typing import Any, cast

from app.application.discovery.discovery_provisioning_service import DiscoveryProvisioningService
from app.application.discovery.discovery_runner import DiscoveryRunnerFactory
from app.application.discovery.metadata_self_healing_service import MetadataSelfHealingService
from app.application.unit_of_work import UnitOfWork
from app.domain.assets.data_asset import DataAsset
from app.domain.discovery.discovery_run import DiscoveryRun
from app.domain.discovery.services.schema_drift_service import SchemaDriftService
from app.domain.shared.exceptions import PlatformNotFoundError

logger = logging.getLogger(__name__)


class RunDiscoveryUseCase:
    def __init__(
        self,
        uow: UnitOfWork,
        runner_factory: DiscoveryRunnerFactory,
        drift_service: SchemaDriftService | None = None,
        self_healing: MetadataSelfHealingService | None = None,
        provisioning_service: DiscoveryProvisioningService | None = None,
        schema_differ: Any | None = None,
        tag_inferrer: Any | None = None,
    ) -> None:
        self._uow = uow
        self._runner_factory = runner_factory

        if drift_service is not None:
            self._drift_service = drift_service
        else:
            from app.domain.discovery.services.policy_tag_inferrer import PolicyTagInferrer
            from app.domain.discovery.services.schema_differ import SchemaDiffer

            s_differ = schema_differ or SchemaDiffer()
            t_inferrer = tag_inferrer or PolicyTagInferrer()
            self._drift_service = SchemaDriftService(s_differ, t_inferrer)

        self._self_healing = self_healing or MetadataSelfHealingService(uow=uow)
        self._provisioning = provisioning_service or DiscoveryProvisioningService(uow=uow)

    async def execute(self, asset_id: str, triggered_by: str) -> DiscoveryRun:
        try:
            async with self._uow as uow:
                # 1. Initialize
                asset = await uow.assets.find_by_id(asset_id)
                self._validate_asset(asset, asset_id)
                assert asset is not None

                endpoint_id = cast(str, asset.endpoint_id)
                endpoint = await uow.endpoints.find_by_id(endpoint_id)
                if not endpoint:
                    raise PlatformNotFoundError(f"Endpoint not found: {endpoint_id}")

                objects = await uow.objects.find_by_asset_id(asset_id)
                run = DiscoveryRun(
                    id=str(uuid.uuid4()), asset_id=asset_id, triggered_by=triggered_by
                )
                run.start()

                run = await uow.discovery_runs.save(run)

                # 2. Extract
                runner = self._runner_factory.create(endpoint)
                scope_include = list(asset.discovery_scope.include) or ["*"]
                scope_exclude = list(asset.discovery_scope.exclude)
                snapshots = await runner.run(asset.id, scope_include, scope_exclude, endpoint)

                # 3. Process
                snapshots = await self._provisioning.provision_missing_objects(
                    asset_id, snapshots, objects
                )

                baseline_run = await uow.discovery_runs.find_latest_by_asset_id(asset_id)
                prev_snapshots = {
                    s.object_id: s for s in (baseline_run.snapshots if baseline_run else [])
                }

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

                await self._self_healing.apply_self_healing_and_approvals(
                    asset_id=asset_id,
                    run_id=run.id,
                    snapshots=snapshots,
                    drift_events=events,
                    prev_snapshots=prev_snapshots,
                )

                await uow.discovery_runs.save(run)
                await uow.commit()

            logger.info(
                "Discovery completed successfully | asset_id=%s | run_id=%s", asset_id, run.id
            )
            return run
        except Exception as e:
            logger.exception(
                "Discovery failed | asset_id=%s | triggered_by=%s", asset_id, triggered_by
            )
            async with self._uow as uow:
                try:
                    run.fail(str(e))
                    await uow.discovery_runs.save(run)
                    await uow.commit()
                except NameError:
                    pass
            raise

    def _validate_asset(self, asset: DataAsset | None, asset_id: str) -> None:
        if not asset:
            raise PlatformNotFoundError(f"Asset not found: {asset_id}")
        if not asset.endpoint_id:
            raise ValueError(f"Asset has no endpoint: {asset_id}")
