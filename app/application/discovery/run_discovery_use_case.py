from __future__ import annotations

import logging
import uuid

from app.application.discovery.discovery_provisioning_service import DiscoveryProvisioningService
from app.application.discovery.discovery_runner import DiscoveryRunnerFactory
from app.application.discovery.metadata_self_healing_service import MetadataSelfHealingService
from app.application.unit_of_work import UnitOfWork
from app.domain.assets.data_asset import DataAsset
from app.domain.discovery.discovery_run import DiscoveryRun
from app.domain.discovery.services.schema_drift_service import SchemaDriftService
from app.domain.shared.exceptions import PlatformNotFoundError, PlatformValidationError

logger = logging.getLogger(__name__)


class RunDiscoveryUseCase:
    def __init__(
        self,
        uow: UnitOfWork,
        runner_factory: DiscoveryRunnerFactory,
        drift_service: SchemaDriftService,
        self_healing: MetadataSelfHealingService,
        provisioning_service: DiscoveryProvisioningService,
    ) -> None:
        self._uow = uow
        self._runner_factory = runner_factory
        self._drift_service = drift_service
        self._self_healing = self_healing
        self._provisioning = provisioning_service

    async def execute(
        self,
        asset_id: str | None = None,
        triggered_by: str = "system",
        *,
        asset_name: str | None = None,
    ) -> DiscoveryRun:
        run: DiscoveryRun | None = None
        try:
            async with self._uow as uow:
                # 1. Initialize
                if asset_name:
                    asset = await uow.assets.find_by_name(asset_name)
                    if not asset:
                        raise PlatformNotFoundError(f"Asset not found: {asset_name}")
                    asset_id = asset.id
                elif asset_id:
                    asset = await uow.assets.find_by_id(asset_id)
                    if not asset:
                        raise PlatformNotFoundError(f"Asset not found: {asset_id}")
                else:
                    raise PlatformValidationError("Either asset_id or asset_name must be provided")

                endpoint_id = self._validate_asset(asset, asset_id)

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
        except KeyError as e:
            logger.exception(
                "Discovery secret or configuration missing | asset_id=%s | triggered_by=%s",
                asset_id,
                triggered_by,
            )
            await self._fail_run_if_started(run, f"Configuration or credential not found: {e}")
            raise PlatformNotFoundError(f"Configuration or credential not found: {e}") from e
        except Exception as e:
            logger.exception(
                "Discovery failed | asset_id=%s | triggered_by=%s", asset_id, triggered_by
            )
            await self._fail_run_if_started(run, str(e))
            raise

    async def _fail_run_if_started(self, run: DiscoveryRun | None, reason: str) -> None:
        """Persiste falha apenas se run foi inicializado antes da excecao."""
        if run is None:
            return
        try:
            async with self._uow as uow:
                run.fail(reason)
                await uow.discovery_runs.save(run)
                await uow.commit()
        except Exception:
            logger.exception("Could not persist discovery run failure | run_id=%s", run.id)

    def _validate_asset(self, asset: DataAsset | None, asset_id: str) -> str:
        if not asset:
            raise PlatformNotFoundError(f"Asset not found: {asset_id}")
        if not asset.endpoint_id:
            raise PlatformValidationError(f"Asset has no endpoint: {asset_id}")
        return asset.endpoint_id
