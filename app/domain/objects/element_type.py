from __future__ import annotations

from enum import StrEnum


class ElementType(StrEnum):
    """Supported data types for DataElement source and destination mapping."""

    STRING = "string"
    INTEGER = "integer"
    BIGINT = "bigint"
    FLOAT = "float"
    DECIMAL = "decimal"
    DATE = "date"
    TIMESTAMP = "timestamp"
    BOOLEAN = "boolean"
    BYTES = "bytes"
    JSON = "json"
