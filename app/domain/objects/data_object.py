from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from app.domain.objects.data_element import DataElement
from app.domain.objects.data_object_metadata import DataObjectMetadata
from app.domain.objects.freshness_status import FreshnessStatus
from app.domain.objects.object_type import ObjectType
from app.domain.shared.auditable import Auditable
from app.domain.shared.policy_tag import PolicyTag


@dataclass(kw_only=True)
class DataObject(Auditable):
    """
    Logical data entity within a DataAsset (table, file, API resource, view, or collection).

    Intentionally has no pipeline_id: a DataObject can participate in multiple pipelines
    (as source in one ingestion, as destination in another ETL, etc.).
    The relationship DataObject <-> Pipeline is managed by PipelineObjectRef (infrastructure).

    freshness_status is calculated by the catalog layer based on last_success vs schedule.
    policy_tags are inherited from the parent DataAsset and refined per DataElement.
    object_metadata holds structural constraints (indexes, FKs) discovered from the source.
    """

    id: str
    asset_id: str
    name: str
    type: ObjectType
    description: str = ""
    policy_tags: list[PolicyTag] = field(default_factory=list)
    last_run: datetime | None = None
    last_success: datetime | None = None
    freshness_status: FreshnessStatus = FreshnessStatus.UNKNOWN
    elements: list[DataElement] = field(default_factory=list)
    auto_generated_description: bool = False
    object_metadata: DataObjectMetadata | None = None

    @classmethod
    def create_from_discovery(cls, asset_id: str, name: str, description: str = "") -> DataObject:
        """Factory for creating a DataObject auto-provisioned during discovery."""
        import uuid

        return cls(
            id=str(uuid.uuid4()),
            asset_id=asset_id,
            name=name,
            type=ObjectType.TABLE,  # Defaulting to table for auto-provisioned objects
            description=description,
            policy_tags=[],
            freshness_status=FreshnessStatus.UNKNOWN,
            elements=[],
            auto_generated_description=True,
            object_metadata=None,
        )
