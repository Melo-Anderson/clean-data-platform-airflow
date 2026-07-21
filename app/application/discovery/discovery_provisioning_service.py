from __future__ import annotations

import uuid

from app.application.unit_of_work import UnitOfWork
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.objects.data_object import DataObject
from app.domain.objects.data_object_metadata import (
    CompositeForeignKey,
    CompositeIndex,
    DataObjectMetadata,
)
from app.domain.objects.object_type import ObjectType


def _build_object_metadata(extra: dict) -> DataObjectMetadata | None:
    """
    Converts the runner-provided `extra` dict from a SchemaSnapshot into
    a DataObjectMetadata Value Object.

    Runners populate `extra` with keys: indexes, foreign_keys, partition_key.
    Returns None when extra is empty or all keys are empty/None.
    """
    if not extra:
        return None
    indexes = [CompositeIndex(**idx) for idx in extra.get("indexes", [])]
    foreign_keys = [CompositeForeignKey(**fk) for fk in extra.get("foreign_keys", [])]
    partition_key = extra.get("partition_key")
    if not indexes and not foreign_keys and partition_key is None:
        return None
    return DataObjectMetadata(
        indexes=indexes, foreign_keys=foreign_keys, partition_key=partition_key
    )


class DiscoveryProvisioningService:
    """
    Application service responsible for checking discovery snapshots against
    existing DataObjects and auto-provisioning missing ones.

    Also synchronizes structural metadata (indexes, FKs, partition key) from
    the runner's SchemaSnapshot.extra into DataObject.object_metadata.
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

        Also updates object_metadata for all objects (new and existing)
        based on the structural information in each snapshot's extra dict.

        Returns a list of updated snapshots containing the correct real object_ids.
        """
        existing_names = {obj.name: obj for obj in existing_objects}

        # 1. Provision missing objects and sync metadata
        for snap in snapshots:
            if snap.object_name not in existing_names:
                new_obj = DataObject(
                    id=str(uuid.uuid4()),
                    asset_id=asset_id,
                    name=snap.object_name,
                    type=ObjectType.TABLE,
                    description="Auto-discovered by discovery run",
                    auto_generated_description=True,
                    object_metadata=_build_object_metadata(snap.extra),
                )
                saved_obj = await self._uow.objects.save(new_obj)
                existing_names[snap.object_name] = saved_obj
            else:
                # Update object_metadata for existing objects on every discovery run
                obj = existing_names[snap.object_name]
                new_metadata = _build_object_metadata(snap.extra)
                if new_metadata is not None and new_metadata != obj.object_metadata:
                    obj.object_metadata = new_metadata
                    obj.touch()
                    await self._uow.objects.save(obj)

        # 2. Update snapshots with real object IDs
        updated_snapshots = []
        for snap in snapshots:
            obj = existing_names[snap.object_name]
            updated_snap = SchemaSnapshot(
                object_id=obj.id,
                fields=snap.fields,
                captured_at=snap.captured_at,
                runner_type=snap.runner_type,
                object_name=snap.object_name,
                row_count_estimate=snap.row_count_estimate,
                extra=snap.extra,
            )
            updated_snapshots.append(updated_snap)

        return updated_snapshots
