from __future__ import annotations

from enum import StrEnum


class DiscoveryRunStatus(StrEnum):
    """
    Lifecycle of a single DiscoveryRun execution.
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"
