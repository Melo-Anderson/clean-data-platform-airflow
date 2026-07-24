from __future__ import annotations

from typing import Protocol, runtime_checkable

from app.domain.objects.data_element import DataElement
from app.domain.objects.data_object import DataObject


@runtime_checkable
class DataObjectRepository(Protocol):
    async def save(self, obj: DataObject) -> DataObject:
        ...

    async def find_by_id(self, object_id: str) -> DataObject | None:
        ...

    async def find_by_asset_id(self, asset_id: str) -> list[DataObject]:
        ...

    async def add_element(self, object_id: str, element: DataElement) -> DataElement:
        ...

    async def update_element_destination_type(
        self, element_id: str, destination_type: str, required: bool, nullable: bool
    ) -> DataElement:
        ...
