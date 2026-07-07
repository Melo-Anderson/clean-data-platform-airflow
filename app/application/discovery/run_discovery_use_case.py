from __future__ import annotations

import uuid
import logging

from app.application.discovery.discovery_runner import DiscoveryRunnerFactory
from app.application.unit_of_work import UnitOfWork
from app.domain.assets.data_asset import DataAsset
from app.domain.discovery.discovery_run import DiscoveryRun
from app.domain.discovery.drift_approval import DriftApproval
from app.domain.discovery.drift_event import DriftEvent
from app.domain.discovery.policy_tag_suggestion import PolicyTagSuggestion
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.discovery.services.policy_tag_inferrer import PolicyTagInferrer
from app.domain.discovery.services.schema_differ import SchemaDiffer
from app.domain.endpoints.endpoint import Endpoint
from app.domain.objects.data_object import DataObject
from app.domain.objects.object_service import DataObjectService


logger = logging.getLogger(__name__)


class RunDiscoveryUseCase:
    """Executes the metadata discovery process for a given DataAsset."""

    def __init__(
        self,
        uow: UnitOfWork,
        runner_factory: DiscoveryRunnerFactory,
        schema_differ: SchemaDiffer,
        tag_inferrer: PolicyTagInferrer,
    ) -> None:
        self._uow = uow
        self._runner_factory = runner_factory
        self._schema_differ = schema_differ
        self._tag_inferrer = tag_inferrer

    async def execute(self, asset_id: str, triggered_by: str) -> DiscoveryRun:
        run, endpoint, asset, objects = await self._initialize_run(asset_id, triggered_by)

        try:
            snapshots = await self._extract_snapshots(endpoint, asset)
            await self._process_discovery_results(asset_id, run, snapshots, objects)
            return run
        except Exception as e:
            logger.exception("Discovery failed")
            await self._fail_run(run, str(e))
            raise

    async def _initialize_run(
        self, asset_id: str, triggered_by: str
    ) -> tuple[DiscoveryRun, Endpoint, DataAsset, list[DataObject]]:
        async with self._uow as uow:
            asset = await uow.assets.find_by_id(asset_id)
            self._validate_asset(asset, asset_id)
            # _validate_asset already checked for None, but Mypy needs an explicit assert
            assert asset is not None
            from typing import cast
            endpoint_id = cast(str, asset.endpoint_id)  # type: ignore[union-attr]

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
        return await runner.run(asset.id, scope_include, endpoint)

    async def _process_discovery_results(
        self, asset_id: str, run: DiscoveryRun, snapshots: list[SchemaSnapshot], objects: list[DataObject]
    ) -> None:
        async with self._uow as uow:
            # Auto-provision missing data objects
            existing_names = {obj.name: obj for obj in objects}
            for snap in snapshots:
                if snap.object_name not in existing_names:
                    # Create new object
                    from app.domain.objects.object_type import ObjectType
                    from app.domain.objects.freshness_status import FreshnessStatus
                    new_obj = DataObject(
                        id=str(uuid.uuid4()),
                        asset_id=asset_id,
                        name=snap.object_name,
                        type=ObjectType.TABLE, # Defaulting to table for auto-provisioned
                        description="",
                        policy_tags=[],
                        last_run=None,
                        last_success=None,
                        freshness_status=FreshnessStatus.UNKNOWN,
                        elements=[],
                        auto_generated_description=True,
                    )
                    saved_obj = await uow.objects.save(new_obj)
                    existing_names[snap.object_name] = saved_obj
                    
            # Update snapshots with real object IDs
            updated_snapshots = []
            for snap in snapshots:
                obj = existing_names[snap.object_name]
                # Replace the snapshot with one that has the object_id
                updated_snap = SchemaSnapshot(
                    object_id=obj.id,
                    fields=snap.fields,
                    captured_at=snap.captured_at,
                    runner_type=snap.runner_type,
                    object_name=snap.object_name,
                    row_count_estimate=snap.row_count_estimate
                )
                updated_snapshots.append(updated_snap)
                
            snapshots = updated_snapshots

            baseline_run = await uow.discovery_runs.find_latest_by_asset_id(asset_id)
            prev_snapshots = {s.object_id: s for s in (baseline_run.snapshots if baseline_run else [])}

            events, suggestions = self._compute_drifts_and_tags(prev_snapshots, snapshots)

            run.complete(
                snapshots=snapshots,
                drift_events=events,
                policy_tag_suggestions=suggestions,
                auto_generated_descriptions={},
                soft_failures=[],
            )

            await self._apply_self_healing_and_approvals(uow, asset_id, run.id, snapshots, events, prev_snapshots)

            await uow.discovery_runs.save(run)
            await uow.commit()

    def _compute_drifts_and_tags(
        self, prev_snapshots: dict[str, SchemaSnapshot], snapshots: list[SchemaSnapshot]
    ) -> tuple[list[DriftEvent], list[PolicyTagSuggestion]]:
        drift_events = []
        suggestions = []

        for snap in snapshots:
            prev = prev_snapshots.get(snap.object_id)
            drift_events.extend(self._schema_differ.diff(prev, snap))
            for field in snap.fields:
                sug = self._tag_inferrer.infer(field.name)
                if sug:
                    suggestions.append(sug)

        return drift_events, suggestions

    async def _apply_self_healing_and_approvals(
        self,
        uow: UnitOfWork,
        asset_id: str,
        run_id: str,
        snapshots: list[SchemaSnapshot],
        drift_events: list[DriftEvent],
        prev_snapshots: dict[str, SchemaSnapshot],
    ) -> None:
        object_service = DataObjectService(uow.objects)

        for snap in snapshots:
            obj_events = [e for e in drift_events if e.object_id == snap.object_id]
            informative_events = [e for e in obj_events if not e.is_critical]
            critical_events = [e for e in obj_events if e.is_critical]

            if informative_events or not prev_snapshots:
                await object_service.apply_schema_snapshot(snap.object_id, snap)

            for evt in critical_events:
                approval = DriftApproval(
                    id=str(uuid.uuid4()),
                    discovery_run_id=run_id,
                    asset_id=asset_id,
                    object_id=snap.object_id,
                    field_name=evt.field_name,
                    change_type=evt.change_type,
                    severity_description=evt.description,
                )
                await uow.drift_approvals.save(approval)

    async def _fail_run(self, run: DiscoveryRun, error_message: str) -> None:
        async with self._uow as uow:
            run.fail(error_message)
            await uow.discovery_runs.save(run)
            await uow.commit()
