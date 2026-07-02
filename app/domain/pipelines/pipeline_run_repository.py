from __future__ import annotations

from typing import Protocol, runtime_checkable

from app.domain.pipelines.pipeline_run import PipelineRun


@runtime_checkable
class PipelineRunRepository(Protocol):
    """
    Repository for PipelineRun persistence.

    last_run_at and last_success_at are maintained as columns on the
    PipelineRunModel to allow efficient dashboard queries without aggregation.
    """

    async def save(self, run: PipelineRun) -> PipelineRun: ...

    async def find_by_id(self, run_id: str) -> PipelineRun | None: ...

    async def find_latest_by_pipeline_id(self, pipeline_id: str) -> PipelineRun | None:
        """Return the most recent run (by started_at desc) for a given pipeline."""
        ...

    async def find_dashboard_summary(self) -> list[dict]:
        """
        Return one row per pipeline with operational dashboard fields:
        {pipeline_id, pipeline_name, status, last_run_at, last_success_at,
         failed_task, sla_breached, duration_seconds}

        Optimized for dashboard reads — does NOT load full metrics JSON.
        """
        ...
