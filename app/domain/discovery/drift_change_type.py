from __future__ import annotations

from enum import StrEnum


class DriftChangeType(StrEnum):
    """
    Specific type of schema change detected between two SchemaSnapshots.
    """

    FIELD_ADDED = "field_added"
    FIELD_REMOVED = "field_removed"
    TYPE_WIDENED = "type_widened"
    TYPE_INCOMPATIBLE = "type_incompatible"
    NULLABLE_TO_REQUIRED = "nullable_to_required"
    REQUIRED_TO_NULLABLE = "required_to_nullable"
    OBJECT_ADDED = "object_added"
    OBJECT_REMOVED = "object_removed"
