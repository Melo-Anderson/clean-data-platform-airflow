from __future__ import annotations

from enum import StrEnum


class ObjectType(StrEnum):
    """Supported DataObject structural types."""

    TABLE = "table"
    VIEW = "view"
    FILE = "file"
    API_RESOURCE = "api_resource"
    COLLECTION = "collection"
