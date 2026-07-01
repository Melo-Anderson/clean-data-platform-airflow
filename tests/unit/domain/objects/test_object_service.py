from __future__ import annotations

import uuid

import pytest

from app.domain.objects.data_element import DataElement
from app.domain.objects.data_object import DataObject
from app.domain.objects.element_type import ElementType
from app.domain.objects.object_service import (
    DataObjectService,
    DestructiveOverrideWarning,
)
from app.domain.objects.object_type import ObjectType
from app.domain.shared.policy_tag import PolicyTag


class FakeDataObjectRepository:
    def __init__(self) -> None:
        self._objects: dict[str, DataObject] = {}
        self._elements: dict[str, DataElement] = {}

    async def save(self, obj: DataObject) -> DataObject:
        self._objects[obj.id] = obj
        return obj

    async def find_by_id(self, object_id: str) -> DataObject | None:
        return self._objects.get(object_id)

    async def find_by_asset_id(self, asset_id: str) -> list[DataObject]:
        return [o for o in self._objects.values() if o.asset_id == asset_id]

    async def add_element(self, object_id: str, element: DataElement) -> DataElement:
        self._elements[element.id] = element
        self._objects[object_id].elements.append(element)
        return element

    async def update_element_destination_type(
        self, element_id: str, destination_type: str, required: bool, nullable: bool
    ) -> DataElement:
        el = self._elements[element_id]
        el.destination_type = ElementType(destination_type)
        el.required = required
        el.nullable = nullable
        return el


def _service() -> tuple[DataObjectService, FakeDataObjectRepository]:
    repo = FakeDataObjectRepository()
    return DataObjectService(repo=repo), repo


@pytest.mark.asyncio
async def test_register_creates_data_object_without_role_or_pipeline() -> None:
    service, _ = _service()
    obj = await service.register(
        object_id=str(uuid.uuid4()),
        asset_id="asset-1",
        name="customers",
        object_type=ObjectType.TABLE,
        description="Customer data",
        policy_tags=[PolicyTag.PII],
    )
    assert obj.name == "customers"
    assert not hasattr(obj, "role"), "DataObject must not have an ObjectRole attribute"
    assert not hasattr(obj, "pipeline_id"), "DataObject must not have a pipeline_id attribute"


@pytest.mark.asyncio
async def test_destructive_override_raises_warning() -> None:
    service, _ = _service()
    obj = await service.register(str(uuid.uuid4()), "a1", "t", ObjectType.TABLE, "", [])
    el = DataElement(
        id=str(uuid.uuid4()),
        object_id=obj.id,
        name="zip",
        source_type=ElementType.STRING,
        destination_type=ElementType.STRING,
    )
    await service.add_element(obj.id, el)
    with pytest.raises(DestructiveOverrideWarning):
        await service.override_element_destination(
            obj.id, el.id, "zip", ElementType.STRING, ElementType.INTEGER, False, True
        )


@pytest.mark.asyncio
async def test_integer_to_bigint_is_not_destructive() -> None:
    service, _ = _service()
    obj = await service.register(str(uuid.uuid4()), "a1", "t", ObjectType.TABLE, "", [])
    el = DataElement(
        id=str(uuid.uuid4()),
        object_id=obj.id,
        name="age",
        source_type=ElementType.INTEGER,
        destination_type=ElementType.INTEGER,
    )
    await service.add_element(obj.id, el)
    updated = await service.override_element_destination(
        obj.id, el.id, "age", ElementType.INTEGER, ElementType.BIGINT, True, False
    )
    assert updated.destination_type == ElementType.BIGINT
