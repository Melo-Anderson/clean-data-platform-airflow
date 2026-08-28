from __future__ import annotations

import fnmatch
import logging
import re
from pathlib import Path

import duckdb

from app.application.discovery.discovery_runner import DiscoveryRunner
from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.endpoints.endpoint import Endpoint, FileSystemEndpoint
from app.domain.shared.file_formats import DEFAULT_FILE_SCOPE_PATTERNS
from app.infrastructure.discovery.filesystem_type_mapper import map_duckdb_type_to_normalized

logger = logging.getLogger(__name__)

DEFAULT_DISCOVERY_SAMPLE_SIZE: int = 10

# Registry mapping file extension -> DuckDB reader expression (extensible for new formats)
_SUPPORTED_FORMAT_READERS: dict[str, str] = {
    ".csv": "read_csv('{path}', header=true, auto_detect=true, sample_size={sample_size})",
    ".tsv": "read_csv('{path}', header=true, auto_detect=true, sample_size={sample_size})",
    ".txt": "read_csv('{path}', header=true, auto_detect=true, sample_size={sample_size})",
    ".json": "read_json('{path}', auto_detect=true, sample_size={sample_size})",
    ".ndjson": "read_json('{path}', auto_detect=true, sample_size={sample_size})",
    ".jsonl": "read_json('{path}', auto_detect=true, sample_size={sample_size})",
    ".parquet": "read_parquet('{path}')",
}

_DEFAULT_SCOPE_PATTERNS: tuple[str, ...] = DEFAULT_FILE_SCOPE_PATTERNS


def _build_duckdb_describe_query(
    file_path: Path, sample_size: int = DEFAULT_DISCOVERY_SAMPLE_SIZE
) -> str | None:
    """Build a DESCRIBE query for a file based on its registered extension reader."""
    reader_expr = _SUPPORTED_FORMAT_READERS.get(file_path.suffix.lower())
    if not reader_expr:
        return None
    p_str = file_path.resolve().as_posix()
    formatted_expr = reader_expr.format(path=p_str, sample_size=sample_size)
    return f"DESCRIBE SELECT * FROM {formatted_expr}"


def _parse_pattern_and_target(entry: str) -> tuple[str, str | None]:
    """Parse entry like '*pedidos*.json:pedidos' or '*pedidos*.json'."""
    if ":" in entry:
        pattern, target = entry.split(":", 1)
        return pattern.strip(), target.strip()
    if "->" in entry:
        pattern, target = entry.split("->", 1)
        return pattern.strip(), target.strip()
    return entry.strip(), None


def _extract_canonical_name(pattern: str, file_path: Path) -> str:
    """Derive canonical DataObject name from pattern or file stem."""
    clean_pat = pattern.replace("*", "").replace("?", "").strip()
    if clean_pat and not clean_pat.startswith("."):
        name = Path(clean_pat).stem
        if name:
            return name

    stem = file_path.stem
    cleaned = re.sub(r"[_\-]\d{4}[\-_]?\d{2}[\-_]?\d{2}(_\d{2,6})?$", "", stem)
    return cleaned or stem


class FileSystemDiscoveryRunner(DiscoveryRunner):
    """
    DiscoveryRunner for local filesystem and mounted storage volumes.
    Uses DuckDB in-memory to sniff schemas of tabular and semi-structured files.
    """

    def __init__(self, sample_size: int = DEFAULT_DISCOVERY_SAMPLE_SIZE) -> None:
        self._sample_size = sample_size

    async def run(
        self,
        asset_id: str,
        scope_include: list[str],
        scope_exclude: list[str],
        endpoint: Endpoint,
    ) -> list[SchemaSnapshot]:
        if not isinstance(endpoint, FileSystemEndpoint):
            raise TypeError(f"Expected FileSystemEndpoint, got {type(endpoint).__name__}")

        root = Path(endpoint.root_path)
        if not root.exists() or not root.is_dir():
            logger.warning("FileSystemDiscoveryRunner: root_path does not exist: %s", root)
            return []

        matched_groups = self._collect_matching_files(root, scope_include, scope_exclude)
        snapshots: list[SchemaSnapshot] = []

        for obj_name, files in matched_groups.items():
            latest_file = max(files, key=lambda f: f.stat().st_mtime)
            snapshot = self._inspect_file(asset_id, obj_name, latest_file)
            if snapshot:
                snapshots.append(snapshot)

        return snapshots

    def _collect_matching_files(
        self,
        root: Path,
        scope_include: list[str],
        scope_exclude: list[str],
    ) -> dict[str, list[Path]]:
        """Collect and group matching files by canonical target name."""
        entries = scope_include if scope_include else list(_DEFAULT_SCOPE_PATTERNS)
        matched_groups: dict[str, list[Path]] = {}

        for entry in entries:
            pattern, explicit_name = _parse_pattern_and_target(entry)
            files = self._find_files_for_pattern(root, pattern, scope_exclude)
            if files:
                if explicit_name:
                    matched_groups.setdefault(explicit_name, []).extend(files)
                else:
                    for f in files:
                        obj_name = _extract_canonical_name(pattern, f)
                        matched_groups.setdefault(obj_name, []).append(f)

        return matched_groups

    def _find_files_for_pattern(
        self,
        root: Path,
        pattern: str,
        scope_exclude: list[str],
    ) -> list[Path]:
        """Find non-excluded files matching a glob pattern relative to root."""
        matched: list[Path] = []
        for file_path in root.glob("**/*"):
            if not file_path.is_file():
                continue
            rel_str = file_path.relative_to(root).as_posix()
            filename = file_path.name
            if not (fnmatch.fnmatch(filename, pattern) or fnmatch.fnmatch(rel_str, pattern)):
                continue
            if any(
                fnmatch.fnmatch(filename, exc) or fnmatch.fnmatch(rel_str, exc)
                for exc in scope_exclude
            ):
                continue
            matched.append(file_path)
        return matched

    def _inspect_file(self, asset_id: str, obj_name: str, file_path: Path) -> SchemaSnapshot | None:
        """Inspect a single file using DuckDB and return a SchemaSnapshot."""
        query = _build_duckdb_describe_query(file_path, sample_size=self._sample_size)
        if not query:
            return None

        conn = duckdb.connect(database=":memory:")
        try:
            rows = conn.execute(query).fetchall()
            fields = [
                SchemaField(
                    name=col[0],
                    source_type=col[1],
                    normalized_type=map_duckdb_type_to_normalized(col[1]),
                    nullable=(col[2] == "YES" if len(col) > 2 and col[2] is not None else True),
                )
                for col in rows
            ]
            p_str = file_path.resolve().as_posix()
            file_format = file_path.suffix.lstrip(".").lower()
            return SchemaSnapshot(
                object_id=f"{asset_id}.{obj_name}",
                object_name=obj_name,
                fields=fields,
                extra={
                    "file_sample": p_str,
                    "file_name": file_path.name,
                    "format": file_format,
                    "columns": [col[0] for col in rows],
                },
                runner_type="file_system",
            )

        except Exception as exc:
            logger.error("Failed to inspect file %s with DuckDB: %s", file_path, exc)
            return None
        finally:
            conn.close()
