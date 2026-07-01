from __future__ import annotations

from dataclasses import dataclass

from app.domain.objects.element_type import ElementType
from app.domain.shared.auditable import Auditable
from app.domain.shared.policy_tag import PolicyTag


@dataclass(kw_only=True)
class DataElement(Auditable):
    """
    A single field or attribute within a DataObject.

    source_type is immutable after Discovery - changing it would break lineage traceability.
    destination_type, required, and nullable can be overridden by the Analytics Engineer
    to enforce schema contracts at the destination independent of the source.

    is_computed=True for ETL-calculated fields with no source counterpart (no source_type).
    auto_generated=True when description or policy_tag was inferred by Discovery, not confirmed.
    """

    id: str
    object_id: str
    name: str
    source_type: ElementType | None  # None for is_computed=True fields
    destination_type: ElementType
    required: bool = False
    nullable: bool = True
    description: str = ""
    policy_tag: PolicyTag | None = None
    auto_generated: bool = False
    is_computed: bool = False
