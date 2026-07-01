from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.objects.data_element import DataElement
from app.domain.objects.data_object import DataObject


class SqlDataObjectRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, obj: DataObject) -> DataObject:
        # Implementation placeholder
        return obj

    async def find_by_id(self, object_id: str) -> DataObject | None:
        # Implementation placeholder
        return None

    async def find_by_asset_id(self, asset_id: str) -> list[DataObject]:
        # Implementation placeholder
        return []

    async def add_element(self, object_id: str, element: DataElement) -> DataElement:
        # Implementation placeholder
        return element

    async def update_element_destination_type(
        self, element_id: str, destination_type: str, required: bool, nullable: bool
    ) -> DataElement:
        # Implementation placeholder
        raise NotImplementedError
