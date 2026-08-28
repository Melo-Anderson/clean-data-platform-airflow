from __future__ import annotations

import fnmatch
import hashlib
import uuid
from datetime import UTC, datetime
from pathlib import Path

from app.application.unit_of_work import UnitOfWork
from app.domain.endpoints.endpoint import FileSystemEndpoint
from app.domain.pipelines.pipeline_run_file import PipelineRunFile
from app.domain.shared.file_formats import DEFAULT_FILE_SCOPE_PATTERNS


def _calculate_file_md5(file_path: Path) -> str:
    """Calculate MD5 hash of a file in streaming chunks."""
    hasher = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


class FileWatermarkResolver:
    """
    Resolves pending files for incremental batch ingestion by comparing modification
    timestamps and MD5 checksums against the historical successful runs.
    """

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def resolve_pending_files(
        self,
        pipeline_id: str,
        run_id: str,
        endpoint: FileSystemEndpoint,
        scope_include: list[str],
        scope_exclude: list[str],
    ) -> list[PipelineRunFile]:
        root = Path(endpoint.root_path)
        if not root.exists() or not root.is_dir():
            return []

        watermark: datetime | None = None
        async with self._uow:
            latest_run = await self._uow.pipeline_runs.find_latest_by_pipeline_id(pipeline_id)
            if latest_run is not None:
                status_str = getattr(latest_run.status, "value", str(latest_run.status)).lower()
                if status_str in ("success", "partial"):
                    watermark = latest_run.finished_at or latest_run.started_at
            processed_hashes = await self._uow.pipeline_runs.find_processed_hashes_by_pipeline(
                pipeline_id
            )

        candidates = self._scan_directory(root, scope_include, scope_exclude, watermark)
        pending_files = self._build_pending_files(candidates, processed_hashes, run_id)

        self._validate_homogeneity(pending_files)
        return pending_files

    def _build_pending_files(
        self, candidates: list[Path], processed_hashes: set[str], run_id: str
    ) -> list[PipelineRunFile]:
        pending_files: list[PipelineRunFile] = []
        for path in candidates:
            md5_hash = _calculate_file_md5(path)
            if md5_hash in processed_hashes:
                continue

            stat = path.stat()
            mtime_dt = datetime.fromtimestamp(stat.st_mtime, tz=UTC)
            pending_files.append(
                PipelineRunFile(
                    id=str(uuid.uuid4()),
                    pipeline_run_id=run_id,
                    file_path=path.resolve().as_posix(),
                    file_name=path.name,
                    file_size_bytes=stat.st_size,
                    mtime=mtime_dt,
                    hash_md5=md5_hash,
                )
            )
        return pending_files

    def _scan_directory(
        self,
        root: Path,
        scope_include: list[str],
        scope_exclude: list[str],
        watermark: datetime | None,
    ) -> list[Path]:
        patterns = scope_include or list(DEFAULT_FILE_SCOPE_PATTERNS)
        matched: list[Path] = []

        for p in root.glob("**/*"):
            if not p.is_file():
                continue
            name = p.name
            rel = p.relative_to(root).as_posix()
            if not any(fnmatch.fnmatch(name, pat) or fnmatch.fnmatch(rel, pat) for pat in patterns):
                continue
            if any(
                fnmatch.fnmatch(name, exc) or fnmatch.fnmatch(rel, exc) for exc in scope_exclude
            ):
                continue
            if watermark:
                wm = watermark if watermark.tzinfo is not None else watermark.replace(tzinfo=UTC)
                file_mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=UTC)
                if file_mtime < wm:
                    continue

            matched.append(p)
        return matched

    def _validate_homogeneity(self, files: list[PipelineRunFile]) -> None:
        """Ensure all files in the batch have the same extension."""
        if not files:
            return
        extensions = {Path(f.file_path).suffix.lower() for f in files}
        if len(extensions) > 1:
            raise ValueError(f"Heterogeneous file batch not allowed: {extensions}")
