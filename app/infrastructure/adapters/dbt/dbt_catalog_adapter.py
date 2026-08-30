from __future__ import annotations

import uuid
from dataclasses import dataclass

from app.application.unit_of_work import UnitOfWork
from app.domain.objects.data_element import DataElement
from app.domain.objects.data_object import DataObject
from app.domain.objects.element_type import ElementType
from app.domain.objects.object_type import ObjectType
from app.infrastructure.adapters.dbt.dbt_manifest_parser import (
    DbtColumnMetadata,
    DbtParsedManifest,
)


@dataclass(frozen=True)
class DbtSyncResult:
    objects_synced: int
    elements_synced: int


class DbtCatalogAdapter:
    """Synchronizes models and columns parsed from dbt manifest into DataObject/DataElement metadata."""

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def sync_manifest(self, asset_id: str, manifest: DbtParsedManifest) -> DbtSyncResult:
        obj_count = 0
        elem_count = 0

        async with self._uow as uow:
            existing_objects = await uow.objects.find_by_asset_id(asset_id)
            existing_by_name = {obj.name: obj for obj in existing_objects}

            for model in manifest.models:
                elements = self._build_elements(model.columns)
                elem_count += len(elements)

                if model.name in existing_by_name:
                    obj = existing_by_name[model.name]
                else:
                    obj = DataObject(
                        id=str(uuid.uuid4()),
                        asset_id=asset_id,
                        name=model.name,
                        type=ObjectType.TABLE,
                        description=model.description or f"dbt model {model.name}",
                        elements=elements,
                    )
                await uow.objects.save(obj)
                obj_count += 1

            await uow.commit()

        return DbtSyncResult(objects_synced=obj_count, elements_synced=elem_count)

    def _build_elements(self, columns: list[DbtColumnMetadata]) -> list[DataElement]:
        elements: list[DataElement] = []
        for col in columns:
            elem_type = self._map_type(col.data_type)
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

    def _map_type(self, raw_type: str) -> ElementType:
        normalized = raw_type.upper()
        if "INT" in normalized:
            return ElementType.INTEGER
        if "NUMERIC" in normalized or "DECIMAL" in normalized or "BIGNUMERIC" in normalized:
            return ElementType.DECIMAL
        if "FLOAT" in normalized:
            return ElementType.FLOAT
        if "TIMESTAMP" in normalized or "DATETIME" in normalized:
            return ElementType.TIMESTAMP
        if "DATE" in normalized:
            return ElementType.DATE
        if "BOOL" in normalized:
            return ElementType.BOOLEAN
        return ElementType.STRING
