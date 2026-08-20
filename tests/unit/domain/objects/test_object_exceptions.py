from __future__ import annotations

import pytest

from app.domain.objects.data_element import DataElement
from app.domain.objects.data_object import DataObject
from app.domain.objects.element_type import ElementType
from app.domain.objects.object_repository import DataObjectRepository
from app.domain.objects.object_service import (
    DataObjectService,
    DestructiveOverrideWarning,
    ObjectNotFoundError,
)
from app.domain.shared.exceptions import PlatformNotFoundError, PlatformValidationError


class FakeDataObjectRepository(DataObjectRepository):
    def __init__(self) -> None:
        self.store: dict[str, DataObject] = {}

    async def save(self, obj: DataObject) -> DataObject:
        self.store[obj.id] = obj
        return obj

    async def find_by_id(self, obj_id: str) -> DataObject | None:
        return self.store.get(obj_id)

    async def find_by_asset_id(self, asset_id: str) -> list[DataObject]:
        return [o for o in self.store.values() if o.asset_id == asset_id]

    async def find_by_name_and_asset(self, name: str, asset_id: str) -> DataObject | None:
        for o in self.store.values():
            if o.name == name and o.asset_id == asset_id:
                return o
        return None

    async def add_element(self, object_id: str, element: DataElement) -> DataElement:
        obj = self.store[object_id]
        obj.elements.append(element)
        return element

    async def update_element_destination_type(
        self, element_id: str, destination_type: str, required: bool, nullable: bool
    ) -> DataElement:
        for obj in self.store.values():
            for el in obj.elements:
                if el.id == element_id:
                    return el
        raise PlatformNotFoundError(f"Element not found: {element_id}")


@pytest.mark.asyncio
async def test_object_not_found_raises_platform_not_found_error():
    service = DataObjectService(repo=FakeDataObjectRepository())
    with pytest.raises(PlatformNotFoundError, match="DataObject not found"):
        await service.add_element(
            "nonexistent",
            DataElement(
                id="e1",
                object_id="nonexistent",
                name="col",
                source_type=ElementType.STRING,
                destination_type=ElementType.STRING,
            ),
        )


def test_destructive_override_warning_is_platform_validation_error():
    err = DestructiveOverrideWarning("col1", ElementType.STRING, ElementType.INTEGER)
    assert isinstance(err, PlatformValidationError)


def test_object_not_found_error_is_platform_not_found_error():
    err = ObjectNotFoundError("obj-123")
    assert isinstance(err, PlatformNotFoundError)
