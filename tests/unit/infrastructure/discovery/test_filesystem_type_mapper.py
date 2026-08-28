from __future__ import annotations

import pytest

from app.infrastructure.discovery.filesystem_type_mapper import map_duckdb_type_to_normalized


@pytest.mark.parametrize(
    "raw_type,expected",
    [
        ("VARCHAR", "string"),
        ("TEXT", "string"),
        ("INTEGER", "integer"),
        ("INT4", "integer"),
        ("BIGINT", "bigint"),
        ("INT8", "bigint"),
        ("FLOAT", "float"),
        ("DOUBLE", "float"),
        ("DECIMAL(10,2)", "decimal"),
        ("BOOLEAN", "boolean"),
        ("DATE", "date"),
        ("TIMESTAMP", "timestamp"),
        ("TIMESTAMP WITH TIME ZONE", "timestamp"),
        ("JSON", "json"),
        ("STRUCT(a INTEGER, b VARCHAR)", "json"),
        ("INTEGER[]", "json"),
        ("UNKNOWN_CUSTOM_TYPE", "string"),
    ],
)
def test_map_duckdb_type_to_normalized(raw_type: str, expected: str) -> None:
    assert map_duckdb_type_to_normalized(raw_type) == expected
