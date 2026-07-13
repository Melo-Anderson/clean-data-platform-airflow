from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.application.unit_of_work import UnitOfWork
from app.domain.pipelines.pipeline_run import PipelineRun
from app.infrastructure.quality_gate_evaluator import QualityGateEvaluator


class ReportPipelineRunUseCase:
    """
    Receives metrics from compute job callback, evaluates quality rules,
    and updates PipelineRun to final status (SUCCESS or QUALITY_FAILED).
    """

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def execute(self, run_id: str, metrics: dict[str, Any]) -> PipelineRun:
        async with self._uow:
            run = await self._uow.pipeline_runs.find_by_id(run_id)
            if run is None:
                raise ValueError(f"PipelineRun not found: {run_id}")
            pipeline = await self._uow.pipelines.find_by_id(run.pipeline_id)
            if pipeline is None:
                raise ValueError(f"Pipeline not found: {run.pipeline_id}")

            quality_rules = [
                {"type": r.type.value, "column": r.column, "value": r.value}
                for r in pipeline.quality_rules
            ]
            evaluator = QualityGateEvaluator()
            violations = evaluator.evaluate(metrics=metrics, rules=quality_rules)
            now = datetime.now(tz=UTC)

            if violations:
                run.mark_quality_failed(finished_at=now, violations=violations)
            else:
                run.mark_success(finished_at=now, metrics=metrics)

            run = await self._uow.pipeline_runs.save(run)
            await self._uow.commit()
        return run
