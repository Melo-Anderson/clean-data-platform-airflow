from __future__ import annotations

from enum import StrEnum


class PipelineRunStatus(StrEnum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    QUALITY_FAILED = "quality_failed"
    PARTIAL = "partial"
