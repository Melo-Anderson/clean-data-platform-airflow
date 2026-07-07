from __future__ import annotations

import json
import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.objects.data_element import DataElement
from app.domain.objects.data_object import DataObject
from app.domain.objects.element_type import ElementType
from app.domain.objects.freshness_status import FreshnessStatus
from app.domain.objects.object_type import ObjectType
from app.domain.shared.policy_tag import PolicyTag
from app.infrastructure.persistence.models.data_element_model import DataElementModel
from app.infrastructure.persistence.models.data_object_model import DataObjectModel


def _element_to_domain(m: DataElementModel) -> DataElement:
    return DataElement(
        id=m.id,
        object_id=m.object_id,
        name=m.name,
        source_type=ElementType(m.source_type) if m.source_type else None,
        destination_type=ElementType(m.destination_type),
        required=m.required,
        nullable=m.nullable,
        description=m.description,
        policy_tag=PolicyTag(m.policy_tag) if m.policy_tag else None,
        auto_generated=m.auto_generated,
        is_computed=m.is_computed,
    )


def _object_to_domain(m: DataObjectModel) -> DataObject:
    policy_tags = [PolicyTag(t) for t in json.loads(m.policy_tags_json or "[]")]
    return DataObject(
        id=m.id,
        asset_id=m.asset_id,
        name=m.name,
        type=ObjectType(m.type),
        description=m.description,
        policy_tags=policy_tags,
        last_run=m.last_run,
        last_success=m.last_success,
        freshness_status=FreshnessStatus(m.freshness_status),
        elements=[_element_to_domain(e) for e in m.elements],
        auto_generated_description=m.auto_generated_description,
        created_at=m.created_at,
        updated_at=m.updated_at,
    )


class SqlDataObjectRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, obj: DataObject) -> DataObject:
        m = await self._session.get(DataObjectModel, obj.id)
        if m is None:
            m = DataObjectModel(id=obj.id)
            self._session.add(m)
        m.asset_id = obj.asset_id
        m.name = obj.name
        m.type = obj.type.value
        m.description = obj.description
        m.policy_tags_json = json.dumps([t.value for t in obj.policy_tags])
        m.last_run = obj.last_run
        m.last_success = obj.last_success
        m.freshness_status = obj.freshness_status.value
        m.auto_generated_description = obj.auto_generated_description
        await self._session.flush()
        await self._session.refresh(m)
        return _object_to_domain(m)

    async def find_by_id(self, object_id: str) -> DataObject | None:
        m = await self._session.get(DataObjectModel, object_id)
        return _object_to_domain(m) if m else None

    async def find_by_asset_id(self, asset_id: str) -> list[DataObject]:
        result = await self._session.execute(
            select(DataObjectModel).where(DataObjectModel.asset_id == asset_id)
        )
        return [_object_to_domain(m) for m in result.scalars().all()]

    async def add_element(self, object_id: str, element: DataElement) -> DataElement:
        m = DataElementModel(
            id=element.id or str(uuid.uuid4()),
            object_id=object_id,
            name=element.name,
            source_type=element.source_type.value if element.source_type else None,
            destination_type=element.destination_type.value,
            required=element.required,
            nullable=element.nullable,
            description=element.description,
            policy_tag=element.policy_tag.value if element.policy_tag else None,
            auto_generated=element.auto_generated,
            is_computed=element.is_computed,
        )
        self._session.add(m)
        await self._session.flush()
        return _element_to_domain(m)

    async def update_element_destination_type(
        self, element_id: str, destination_type: str, required: bool, nullable: bool
    ) -> DataElement:
        m = await self._session.get(DataElementModel, element_id)
        if m is None:
            raise ValueError(f"DataElement not found: {element_id}")
        m.destination_type = destination_type
        m.required = required
        m.nullable = nullable
        await self._session.flush()
        return _element_to_domain(m)
