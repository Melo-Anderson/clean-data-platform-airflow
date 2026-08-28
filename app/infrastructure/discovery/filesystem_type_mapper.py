from __future__ import annotations

import re

_DUCKDB_TYPE_MAP: dict[str, str] = {
    "varchar": "string",
    "text": "string",
    "string": "string",
    "char": "string",
    "bpchar": "string",
    "integer": "integer",
    "int": "integer",
    "int4": "integer",
    "smallint": "integer",
    "int2": "integer",
    "tinyint": "integer",
    "bigint": "bigint",
    "int8": "bigint",
    "hugeint": "bigint",
    "float": "float",
    "float4": "float",
    "double": "float",
    "float8": "float",
    "real": "float",
    "decimal": "decimal",
    "numeric": "decimal",
    "boolean": "boolean",
    "bool": "boolean",
    "date": "date",
    "timestamp": "timestamp",
    "timestamp_s": "timestamp",
    "timestamp_ms": "timestamp",
    "timestamp_ns": "timestamp",
    "timestamptz": "timestamp",
    "time": "string",
    "blob": "string",
    "json": "json",
}


def map_duckdb_type_to_normalized(raw_type: str) -> str:
    """Map a raw DuckDB type string to the platform's canonical ElementType."""
    clean = raw_type.strip().lower()
    base = re.split(r"[\s\(]", clean)[0]

    if clean.endswith("[]") or clean.startswith("struct") or clean.startswith("map"):
        return "json"
    if clean.startswith("list"):
        return "json"
    if "timestamp" in clean:
        return "timestamp"

    return _DUCKDB_TYPE_MAP.get(base, "string")
