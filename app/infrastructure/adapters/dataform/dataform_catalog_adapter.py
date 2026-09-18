from __future__ import annotations

import uuid
from dataclasses import dataclass

from app.application.unit_of_work import UnitOfWork
from app.domain.objects.data_element import DataElement
from app.domain.objects.data_object import DataObject
from app.domain.objects.object_type import ObjectType
from app.infrastructure.adapters.dataform.dataform_compilation_parser import (
    DataformColumnMetadata,
    DataformParsedMetadata,
    DataformTableMetadata,
    map_dataform_type_to_element_type,
)


@dataclass(frozen=True)
class DataformSyncResult:
    objects_synced: int
    elements_synced: int


class DataformCatalogAdapter:
    """Synchronizes models and columns parsed from Dataform compilation into DataObject/DataElement metadata."""

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def sync_metadata(
        self, asset_id: str, metadata: DataformParsedMetadata
    ) -> DataformSyncResult:
        obj_count = 0
        elem_count = 0

        async with self._uow as uow:
            existing_objects = await uow.objects.find_by_asset_id(asset_id)
            existing_by_name = {obj.name: obj for obj in existing_objects}

            all_tables = metadata.tables + metadata.declarations
            for table in all_tables:
                elements = self._build_elements(table.columns)
                elem_count += len(elements)

                obj = self._resolve_or_create_object(
                    table=table,
                    asset_id=asset_id,
                    existing_by_name=existing_by_name,
                    elements=elements,
                )
                saved_obj = await uow.objects.save(obj)
                for el in elements:
                    await uow.objects.add_element(saved_obj.id, el)
                obj_count += 1

            await uow.commit()

        return DataformSyncResult(objects_synced=obj_count, elements_synced=elem_count)

    def _resolve_or_create_object(
        self,
        table: DataformTableMetadata,
        asset_id: str,
        existing_by_name: dict[str, DataObject],
        elements: list[DataElement],
    ) -> DataObject:
        if table.name in existing_by_name:
            return existing_by_name[table.name]

        resolved_type = ObjectType.VIEW if table.type == "view" else ObjectType.TABLE
        return DataObject(
            id=str(uuid.uuid4()),
            asset_id=asset_id,
            name=table.name,
            type=resolved_type,
            description=table.description or f"Dataform model {table.name}",
            elements=elements,
        )

    def _build_elements(self, columns: list[DataformColumnMetadata]) -> list[DataElement]:
        elements: list[DataElement] = []
        for col in columns:
            elem_type = map_dataform_type_to_element_type(col.data_type)
            elements.append(
                DataElement(
                    id=str(uuid.uuid4()),
                    object_id="",
                    name=col.name,
                    source_type=elem_type,
                    destination_type=elem_type,
                    description=col.description,
                    auto_generated=True,
                )
            )
        return elements
