from __future__ import annotations

# OpenAPI primitive type + format → platform normalized_type string
OPENAPI_TYPE_MAP: dict[tuple[str, str], str] = {
    ("integer", ""): "integer",
    ("integer", "int32"): "integer",
    ("integer", "int64"): "bigint",
    ("number", ""): "float",
    ("number", "float"): "float",
    ("number", "double"): "decimal",
    ("boolean", ""): "boolean",
    ("string", ""): "string",
    ("string", "uuid"): "string",
    ("string", "date-time"): "timestamp",
    ("string", "date"): "date",
    ("string", "binary"): "bytes",
    ("array", ""): "json",
    ("object", ""): "json",
}

# Python runtime type → platform normalized_type string (payload sampling fallback)
PYTHON_TYPE_MAP: dict[type, str] = {
    int: "integer",
    float: "float",
    bool: "boolean",
    str: "string",
    dict: "json",
    list: "json",
    bytes: "bytes",
}

WRAPPER_KEYS = ("data", "items", "results", "records", "content")
