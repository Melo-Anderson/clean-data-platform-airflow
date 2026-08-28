from __future__ import annotations

from app.domain.shared.file_formats import (
    DEFAULT_FILE_SCOPE_PATTERNS,
    SUPPORTED_FILE_EXTENSIONS,
    normalize_file_format,
)


def test_supported_file_extensions_and_patterns() -> None:
    assert ".csv" in SUPPORTED_FILE_EXTENSIONS
    assert ".json" in SUPPORTED_FILE_EXTENSIONS
    assert ".parquet" in SUPPORTED_FILE_EXTENSIONS
    assert "*.csv" in DEFAULT_FILE_SCOPE_PATTERNS
    assert "*.json" in DEFAULT_FILE_SCOPE_PATTERNS


def test_normalize_file_format() -> None:
    assert normalize_file_format("orders.csv") == "csv"
    assert normalize_file_format(".csv") == "csv"
    assert normalize_file_format("events.json") == "jsonl"
    assert normalize_file_format("events.ndjson") == "jsonl"
    assert normalize_file_format("events.jsonl") == "jsonl"
    assert normalize_file_format("data.parquet") == "parquet"
    assert normalize_file_format("unknown.xyz") == "csv"
