from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from app.application.unit_of_work import UnitOfWork
from app.domain.pipelines.pipeline_run import PipelineRun
from app.domain.pipelines.pipeline_run_file import PipelineRunFile
from app.domain.pipelines.pipeline_run_status import PipelineRunStatus


class RecordPipelineRunUseCase:
    """Use Case to persist a PipelineRun operational record and its physical files."""

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def execute(
        self,
        *,
        pipeline_id: str,
        pipeline_name: str,
        run_id: str | None = None,
        pipeline_type: str = "ingestion",
        dag_run_id: str = "unknown",
        status: str = "success",
        started_at: datetime,
        finished_at: datetime | None = None,
        failed_task: str | None = None,
        optional_failures: list[str] | None = None,
        quality_violations: list[str] | None = None,
        metrics: dict[str, Any] | None = None,
        sla_minutes: int = 90,
        sla_breached: bool = False,
        files: list[PipelineRunFile] | None = None,
    ) -> PipelineRun:
        async with self._uow:
            try:
                status_enum = PipelineRunStatus(status)
            except ValueError:
                status_enum = PipelineRunStatus.SUCCESS

            run = PipelineRun(
                id=run_id or str(uuid.uuid4()),
                pipeline_id=pipeline_id,
                pipeline_name=pipeline_name,
                pipeline_type=pipeline_type,
                dag_run_id=dag_run_id,
                status=status_enum,
                started_at=started_at,
                finished_at=finished_at,
                failed_task=failed_task,
                optional_failures=optional_failures or [],
                quality_violations=quality_violations or [],
                metrics=metrics or {},
                sla_minutes=sla_minutes,
                sla_breached=sla_breached,
            )

            saved_run = await self._uow.pipeline_runs.save(run)
            if files:
                for f in files:
                    f.pipeline_run_id = saved_run.id
                await self._uow.pipeline_runs.save_files(files)

            await self._uow.commit()
            return saved_run
