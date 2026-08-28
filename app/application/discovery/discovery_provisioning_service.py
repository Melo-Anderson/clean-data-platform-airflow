from __future__ import annotations

import uuid

from app.application.unit_of_work import UnitOfWork
from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.objects.data_element import DataElement
from app.domain.objects.data_object import DataObject
from app.domain.objects.data_object_metadata import (
    CompositeForeignKey,
    CompositeIndex,
    DataObjectMetadata,
)
from app.domain.objects.element_type import ElementType
from app.domain.objects.object_type import ObjectType


def _build_object_metadata(extra: dict) -> DataObjectMetadata | None:
    """
    Converts the runner-provided `extra` dict from a SchemaSnapshot into
    a DataObjectMetadata Value Object.

    Extracts indexes, foreign_keys, partition_key, and stores any additional runner
    context into custom_properties.
    """
    if not extra:
        return None
    indexes = [CompositeIndex(**idx) for idx in extra.get("indexes", [])]
    foreign_keys = [CompositeForeignKey(**fk) for fk in extra.get("foreign_keys", [])]
    partition_key = extra.get("partition_key")

    excluded_keys = {"indexes", "foreign_keys", "partition_key"}
    custom_props = {k: v for k, v in extra.items() if k not in excluded_keys and v is not None}

    return DataObjectMetadata(
        indexes=indexes,
        foreign_keys=foreign_keys,
        partition_key=partition_key,
        custom_properties=custom_props,
    )


def _build_data_elements(object_id: str, fields: list[SchemaField]) -> list[DataElement]:
    """Converts schema fields from a snapshot into DataElement entities."""
    elements: list[DataElement] = []
    for f in fields:
        raw_type = getattr(f, "normalized_type", "string")
        try:
            elem_type = ElementType(raw_type)
        except ValueError:
            elem_type = ElementType.STRING

        elements.append(
            DataElement(
                id=str(uuid.uuid4()),
                object_id=object_id,
                name=f.name,
                source_type=elem_type,
                destination_type=elem_type,
                nullable=getattr(f, "nullable", True),
                is_primary_key=getattr(f, "is_primary_key", False),
                auto_generated=True,
            )
        )
    return elements


class DiscoveryProvisioningService:
    """
    Application service responsible for checking discovery snapshots against
    existing DataObjects and auto-provisioning missing ones.

    Also synchronizes structural metadata (indexes, FKs, partition key, file properties)
    and column-level data elements from the runner's SchemaSnapshot into DataObject.
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
        a new DataObject is created along with its DataElements and metadata.

        Returns a list of updated snapshots containing the correct real object_ids.
        """
        existing_names = {obj.name: obj for obj in existing_objects}

        # 1. Provision missing objects
        for snap in snapshots:
            target_name = snap.extra.get("full_name") or snap.object_name
            obj_id = str(uuid.uuid4())
            if target_name not in existing_names:
                new_obj = DataObject(
                    id=obj_id,
                    asset_id=asset_id,
                    name=target_name,
                    type=ObjectType.TABLE,
                    description="Auto-discovered by discovery run",
                    auto_generated_description=True,
                    elements=_build_data_elements(obj_id, snap.fields),
                    object_metadata=_build_object_metadata(snap.extra),
                )
                saved_obj = await self._uow.objects.save(new_obj)
                existing_names[target_name] = saved_obj

        # 2. Update snapshots with real object IDs
        updated_snapshots = []
        for snap in snapshots:
            target_name = snap.extra.get("full_name") or snap.object_name
            obj = existing_names[target_name]
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
