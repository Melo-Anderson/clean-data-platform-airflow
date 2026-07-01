from __future__ import annotations

from app.domain.objects.data_element import DataElement
from app.domain.objects.data_object import DataObject
from app.domain.objects.element_type import ElementType
from app.domain.objects.object_repository import DataObjectRepository
from app.domain.objects.object_type import ObjectType
from app.domain.shared.policy_tag import PolicyTag

# Type pairs where destination_type override is considered destructive (lossy cast)
_DESTRUCTIVE_OVERRIDES: frozenset[tuple[ElementType, ElementType]] = frozenset(
    {
        (ElementType.STRING, ElementType.INTEGER),
        (ElementType.STRING, ElementType.BIGINT),
        (ElementType.STRING, ElementType.FLOAT),
        (ElementType.STRING, ElementType.DATE),
        (ElementType.STRING, ElementType.TIMESTAMP),
        (ElementType.STRING, ElementType.BOOLEAN),
    }
)


class ObjectNotFoundError(Exception):
    def __init__(self, object_id: str) -> None:
        super().__init__(f"DataObject not found: id={object_id!r}")
        self.object_id = object_id


class DestructiveOverrideWarning(Exception):
    """
    Raised when destination_type is type-incompatible with source_type.

    This is a warning-level validation: CI flags it and blocks deploy
    until the Analytics Engineer explicitly confirms the override.
    """

    def __init__(self, element_name: str, source: ElementType, destination: ElementType) -> None:
        super().__init__(
            f"Destructive type override on element '{element_name}': "
            f"source_type={source!r} -> destination_type={destination!r}. "
            "Explicit AE confirmation required before deploy."
        )


class DataObjectService:
    """
    Domain service for DataObject and DataElement management.
    No FastAPI. No SQLAlchemy. Depends only on DataObjectRepository Protocol.
    """

    def __init__(self, repo: DataObjectRepository) -> None:
        self._repo = repo

    async def register(
        self,
        object_id: str,
        asset_id: str,
        name: str,
        object_type: ObjectType,
        description: str,
        policy_tags: list[PolicyTag],
    ) -> DataObject:
        obj = DataObject(
            id=object_id,
            asset_id=asset_id,
            name=name,
            type=object_type,
            description=description,
            policy_tags=policy_tags,
        )
        return await self._repo.save(obj)

    async def add_element(self, object_id: str, element: DataElement) -> DataElement:
        await self._require_object(object_id)
        return await self._repo.add_element(object_id, element)

    async def override_element_destination(
        self,
        object_id: str,
        element_id: str,
        element_name: str,
        source_type: ElementType | None,
        destination_type: ElementType,
        required: bool,
        nullable: bool,
    ) -> DataElement:
        """
        Override destination_type for a DataElement.

        Raises DestructiveOverrideWarning if source->destination is a known lossy cast.
        The caller (router or CI validator) decides whether to allow or reject.
        """
        await self._require_object(object_id)
        if source_type is not None and (source_type, destination_type) in _DESTRUCTIVE_OVERRIDES:
            raise DestructiveOverrideWarning(element_name, source_type, destination_type)
        return await self._repo.update_element_destination_type(
            element_id, destination_type.value, required, nullable
        )

    async def _require_object(self, object_id: str) -> DataObject:
        obj = await self._repo.find_by_id(object_id)
        if obj is None:
            raise ObjectNotFoundError(object_id)
        return obj
