from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.domain.pipelines.pipeline_run_status import PipelineRunStatus
from app.domain.shared.auditable import Auditable


@dataclass(kw_only=True)
class PipelineRun(Auditable):
    """
    Operational execution record for a Pipeline DAG run.

    Created when emit_monitoring_and_sla fires (trigger_rule=all_done).
    Always persisted — even if the pipeline failed — so the dashboard always has
    a record of the last attempt (last_run_at) vs the last success (last_success_at).

    Enables operational dashboards showing:
    - Pipeline health at a glance (status per pipeline)
    - Time since last success (freshness SLA)
    - Failure triage (failed_task for root cause drill-down)
    - Quality trends (quality_violations over time)
    - Optional task degradation (partial runs where observability failed)

    PipelineRun is written by the Airflow callback layer (emit_monitoring_and_sla)
    and read by the platform API for dashboard endpoints.
    It is NOT part of the core data processing path.
    """

    id: str
    pipeline_id: str
    pipeline_name: str
    pipeline_type: str  # "ingestion" | "etl" | "export"
    dag_run_id: str  # Airflow DAG run ID for tracing
    status: PipelineRunStatus
    started_at: datetime
    finished_at: datetime | None = None
    failed_task: str | None = None  # Task ID of the first mandatory task failure
    optional_failures: list[str] = field(default_factory=list)  # Optional tasks that soft_failed
    quality_violations: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(
        default_factory=dict
    )  # rows_written, bytes_written, checksum, etc.
    sla_breached: bool = False
    sla_minutes: int = 90

    def is_partial(self) -> bool:
        """True when mandatory tasks succeeded but optional tasks soft_failed."""
        return self.status == PipelineRunStatus.PARTIAL

    def duration_seconds(self) -> float | None:
        """Elapsed seconds between started_at and finished_at."""
        if self.finished_at is None:
            return None
        return (self.finished_at - self.started_at).total_seconds()

    def mark_success(self, finished_at: datetime, metrics: dict) -> None:
        """Transition to SUCCESS (or PARTIAL if optional_failures is not empty)."""
        self.finished_at = finished_at
        self.metrics = metrics
        self.status = (
            PipelineRunStatus.PARTIAL if self.optional_failures else PipelineRunStatus.SUCCESS
        )
        self.touch()

    def mark_failed(self, finished_at: datetime, failed_task: str) -> None:
        """Transition to FAILED after a mandatory task failure."""
        self.finished_at = finished_at
        self.failed_task = failed_task
        self.status = PipelineRunStatus.FAILED
        self.touch()

    def mark_quality_failed(self, finished_at: datetime, violations: list[str]) -> None:
        """Transition to QUALITY_FAILED after quality_gate rejects the data."""
        self.finished_at = finished_at
        self.quality_violations = violations
        self.status = PipelineRunStatus.QUALITY_FAILED
        self.touch()
