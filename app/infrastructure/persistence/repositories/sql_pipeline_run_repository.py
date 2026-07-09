from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.pipelines.pipeline_run import PipelineRun
from app.domain.pipelines.pipeline_run_status import PipelineRunStatus
from app.infrastructure.persistence.models.pipeline_run_model import PipelineRunModel


def _to_domain(m: PipelineRunModel) -> PipelineRun:
    return PipelineRun(
        id=m.id,
        pipeline_id=m.pipeline_id,
        pipeline_name=m.pipeline_name,
        pipeline_type=m.pipeline_type,
        dag_run_id=m.dag_run_id,
        status=PipelineRunStatus(m.status),
        started_at=m.started_at,
        finished_at=m.finished_at,
        failed_task=m.failed_task,
        optional_failures=m.optional_failures,
        quality_violations=m.quality_violations,
        metrics=m.metrics,
        sla_breached=m.sla_breached,
        sla_minutes=m.sla_minutes,
        created_at=m.created_at,
        updated_at=m.updated_at,
    )


class SqlPipelineRunRepository:
    """
    SQLAlchemy implementation of PipelineRunRepository.

    last_run_at is always set to now().
    last_success_at is updated only for SUCCESS and PARTIAL statuses.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, run: PipelineRun) -> PipelineRun:
        now = datetime.now(tz=UTC)
        
        # Check if model already exists in session or database
        model = await self._session.get(PipelineRunModel, run.id)
        
        if model is not None:
            # Update existing record
            model.pipeline_id = run.pipeline_id
            model.pipeline_name = run.pipeline_name
            model.pipeline_type = run.pipeline_type
            model.dag_run_id = run.dag_run_id
            model.status = run.status.value
            model.started_at = run.started_at
            model.finished_at = run.finished_at
            model.last_run_at = now
            if run.status in (PipelineRunStatus.SUCCESS, PipelineRunStatus.PARTIAL):
                model.last_success_at = now
            model.failed_task = run.failed_task
            model.optional_failures = run.optional_failures
            model.quality_violations = run.quality_violations
            model.metrics = run.metrics
            model.sla_breached = run.sla_breached
            model.sla_minutes = run.sla_minutes
        else:
            # Create new record
            model = PipelineRunModel(
                id=run.id,
                pipeline_id=run.pipeline_id,
                pipeline_name=run.pipeline_name,
                pipeline_type=run.pipeline_type,
                dag_run_id=run.dag_run_id,
                status=run.status.value,
                started_at=run.started_at,
                finished_at=run.finished_at,
                last_run_at=now,
                last_success_at=(
                    now
                    if run.status in (PipelineRunStatus.SUCCESS, PipelineRunStatus.PARTIAL)
                    else None
                ),
                failed_task=run.failed_task,
                optional_failures=run.optional_failures,
                quality_violations=run.quality_violations,
                metrics=run.metrics,
                sla_breached=run.sla_breached,
                sla_minutes=run.sla_minutes,
            )
            self._session.add(model)

        await self._session.flush()
        await self._session.refresh(model)
        return _to_domain(model)

    async def find_by_id(self, run_id: str) -> PipelineRun | None:
        result = await self._session.execute(
            select(PipelineRunModel).where(PipelineRunModel.id == run_id)
        )
        m = result.scalar_one_or_none()
        return _to_domain(m) if m else None

    async def find_latest_by_pipeline_id(self, pipeline_id: str) -> PipelineRun | None:
        result = await self._session.execute(
            select(PipelineRunModel)
            .where(PipelineRunModel.pipeline_id == pipeline_id)
            .order_by(PipelineRunModel.started_at.desc())
            .limit(1)
        )
        m = result.scalar_one_or_none()
        return _to_domain(m) if m else None

    async def find_dashboard_summary(self) -> list[dict[str, Any]]:
        """
        Return one lightweight row per pipeline for the operational dashboard.

        Does NOT load metrics JSON — only the fields needed for at-a-glance health.
        """
        result = await self._session.execute(
            select(
                PipelineRunModel.pipeline_id,
                PipelineRunModel.pipeline_name,
                PipelineRunModel.status,
                PipelineRunModel.last_run_at,
                PipelineRunModel.last_success_at,
                PipelineRunModel.failed_task,
                PipelineRunModel.sla_breached,
                PipelineRunModel.started_at,
                PipelineRunModel.finished_at,
            )
            .distinct(PipelineRunModel.pipeline_id)
            .order_by(
                PipelineRunModel.pipeline_id,
                PipelineRunModel.started_at.desc(),
            )
        )
        return [row._asdict() for row in result.all()]
