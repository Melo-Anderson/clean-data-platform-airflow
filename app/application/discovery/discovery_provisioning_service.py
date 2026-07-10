from __future__ import annotations

from app.application.unit_of_work import UnitOfWork
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.objects.data_object import DataObject


class DiscoveryProvisioningService:
    """
    Application service responsible for checking discovery snapshots against
    existing DataObjects and auto-provisioning missing ones.
    """

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def provision_missing_objects(
        self,
        asset_id: str,
        snapshots: list[SchemaSnapshot],
        existing_objects: list[DataObject],
    ) -> list[SchemaSnapshot]:
        """
        Ensures that every snapshot correlates to an existing DataObject.
        If a table/object was discovered that doesn't exist in the catalog,
        a new DataObject is created.

        Returns a list of updated snapshots containing the correct real object_ids.
        """
        existing_names = {obj.name: obj for obj in existing_objects}

        import uuid

        from app.domain.objects.object_type import ObjectType

        # 1. Provision missing objects
        for snap in snapshots:
            if snap.object_name not in existing_names:
                new_obj = DataObject(
                    id=str(uuid.uuid4()),
                    asset_id=asset_id,
                    name=snap.object_name,
                    type=ObjectType.TABLE,
                    description="Auto-discovered by discovery run",
                    auto_generated_description=True,
                )
                saved_obj = await self._uow.objects.save(new_obj)
                existing_names[snap.object_name] = saved_obj

        # 2. Update snapshots with real object IDs
        updated_snapshots = []
        for snap in snapshots:
            obj = existing_names[snap.object_name]
            # Replace the snapshot with one that has the real object_id
            updated_snap = SchemaSnapshot(
                object_id=obj.id,
                fields=snap.fields,
                captured_at=snap.captured_at,
                runner_type=snap.runner_type,
                object_name=snap.object_name,
                row_count_estimate=snap.row_count_estimate,
            )
            updated_snapshots.append(updated_snap)

        return updated_snapshots
