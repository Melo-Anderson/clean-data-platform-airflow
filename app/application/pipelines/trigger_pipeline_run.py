from __future__ import annotations

import uuid
from datetime import UTC, datetime

from app.application.pipelines.orchestrator_port import OrchestratorPort
from app.application.unit_of_work import UnitOfWork
from app.domain.pipelines.pipeline_run import PipelineRun
from app.domain.pipelines.pipeline_run_status import PipelineRunStatus


class TriggerPipelineRunUseCase:
    def __init__(self, uow: UnitOfWork, orchestrator: OrchestratorPort) -> None:
        self._uow = uow
        self._orchestrator = orchestrator

    async def execute(self, pipeline_id: str, triggered_by: str) -> PipelineRun:
        async with self._uow:
            pipeline = await self._uow.pipelines.find_by_id(pipeline_id)
            if pipeline is None:
                raise ValueError(f"Pipeline not found: {pipeline_id}")

            run_id = str(uuid.uuid4())
            dag_run_id = f"{triggered_by}__{datetime.now(tz=UTC).isoformat()}"
            run = PipelineRun(
                id=run_id,
                pipeline_id=pipeline.id,
                pipeline_name=pipeline.name,
                pipeline_type=pipeline.type.value,
                dag_run_id=dag_run_id,
                status=PipelineRunStatus.RUNNING,
                started_at=datetime.now(tz=UTC),
            )
            run = await self._uow.pipeline_runs.save(run)
            await self._uow.commit()

        await self._orchestrator.trigger_dag(
            pipeline_id=pipeline.id,
            run_id=run.id,
            dag_run_id=dag_run_id,
        )
        return run
