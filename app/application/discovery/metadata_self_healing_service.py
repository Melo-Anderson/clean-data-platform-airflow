from __future__ import annotations

import uuid

from app.application.unit_of_work import UnitOfWork
from app.domain.discovery.drift_approval import DriftApproval
from app.domain.discovery.drift_event import DriftEvent
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.objects.object_service import DataObjectService


class MetadataSelfHealingService:
    """Application service that applies metadata self-healing and persists drift approvals.

    Responsibilities:
    - Apply schema snapshots to DataObjects for informative drift events.
    - Create DriftApproval records for critical drift events requiring human review.
    - Apply schema snapshot on first discovery (no previous snapshots).

    Does NOT compute drifts — that is SchemaDriftService's responsibility.
    """

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def apply_self_healing_and_approvals(
        self,
        asset_id: str,
        run_id: str,
        snapshots: list[SchemaSnapshot],
        drift_events: list[DriftEvent],
        prev_snapshots: dict[str, SchemaSnapshot],
    ) -> None:
        """Apply self-healing updates and generate drift approvals.

        Args:
            asset_id: ID of the DataAsset being discovered.
            run_id: ID of the current DiscoveryRun.
            snapshots: Current discovery snapshots.
            drift_events: All drift events computed for this run.
            prev_snapshots: Map of object_id -> previous SchemaSnapshot.
        """
        object_service = DataObjectService(self._uow.objects)

        for snap in snapshots:
            obj_events = [e for e in drift_events if e.object_id == snap.object_id]
            informative_events = [e for e in obj_events if not e.is_critical]
            critical_events = [e for e in obj_events if e.is_critical]

            # Apply schema healing for informative drift or first-time discovery
            if informative_events or not prev_snapshots:
                await object_service.apply_schema_snapshot(snap.object_id, snap)

            # Persist approval records for critical events (require PO_PM human review)
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
                await self._uow.drift_approvals.save(approval)
