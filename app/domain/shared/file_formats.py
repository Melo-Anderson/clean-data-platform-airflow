from __future__ import annotations

from pathlib import Path

# Canonical supported file extensions across the data platform
SUPPORTED_FILE_EXTENSIONS: tuple[str, ...] = (
    ".csv",
    ".tsv",
    ".txt",
    ".json",
    ".ndjson",
    ".jsonl",
    ".parquet",
)

# Default glob search patterns for discovery and watermark scanners
DEFAULT_FILE_SCOPE_PATTERNS: tuple[str, ...] = tuple(f"*{ext}" for ext in SUPPORTED_FILE_EXTENSIONS)

_EXTENSION_FORMAT_MAP: dict[str, str] = {
    ".csv": "csv",
    ".tsv": "csv",
    ".txt": "csv",
    ".json": "jsonl",
    ".ndjson": "jsonl",
    ".jsonl": "jsonl",
    ".parquet": "parquet",
}


def normalize_file_format(file_path_or_ext: str) -> str:
    """Derive canonical storage/OmniBeam format from a file path or extension."""
    clean = file_path_or_ext.strip().lower()
    suffix = Path(clean).suffix.lower() if "." in clean else f".{clean}"
    return _EXTENSION_FORMAT_MAP.get(suffix, "csv")
