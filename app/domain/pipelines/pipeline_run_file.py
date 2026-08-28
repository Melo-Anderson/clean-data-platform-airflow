from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from app.domain.shared.auditable import Auditable


@dataclass(kw_only=True)
class PipelineRunFile(Auditable):
    """Tracks a physical file included in a PipelineRun batch execution."""

    id: str
    pipeline_run_id: str
    file_path: str
    file_name: str
    file_size_bytes: int
    mtime: datetime
    hash_md5: str
    status: str = "PENDING"
    processed_at: datetime | None = None

    def mark_processed(self) -> None:
        self.status = "PROCESSED"
        self.processed_at = datetime.now(tz=UTC)
        self.touch()

    def mark_failed(self) -> None:
        self.status = "FAILED"
        self.touch()
