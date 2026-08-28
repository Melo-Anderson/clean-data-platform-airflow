from __future__ import annotations

import logging
import pathlib
import uuid
from datetime import UTC, datetime

from app.application.pipelines.orchestrator_port import OrchestratorPort
from app.application.shared.ports.generator_ports import DagGeneratorPort, YamlGeneratorPort
from app.application.shared.ports.telemetry_port import TelemetryPort
from app.application.unit_of_work import UnitOfWork
from app.domain.pipelines.pipeline_run import PipelineRun
from app.domain.pipelines.pipeline_run_status import PipelineRunStatus
from app.domain.shared.exceptions import PlatformNotFoundError

logger = logging.getLogger(__name__)


class TriggerPipelineRunUseCase:
    def __init__(
        self,
        uow: UnitOfWork,
        orchestrator: OrchestratorPort,
        yaml_generator: YamlGeneratorPort,
        dag_generator: DagGeneratorPort,
        dags_path: str = "/opt/airflow/dags",
        telemetry: TelemetryPort | None = None,
    ) -> None:

        self._uow = uow
        self._orchestrator = orchestrator
        self._yaml_generator = yaml_generator
        self._dag_generator = dag_generator
        self._dags_path = dags_path
        self._telemetry = telemetry

    async def execute(self, pipeline_id: str, triggered_by: str) -> PipelineRun:
        async with self._uow:
            pipeline = await self._uow.pipelines.find_by_id(pipeline_id)
            if pipeline is None:
                raise PlatformNotFoundError(f"Pipeline not found: {pipeline_id}")

            run_id = str(uuid.uuid4())
            dag_run_id = f"{triggered_by}__{datetime.now(tz=UTC).strftime('%Y%m%d_%H%M%S_%f')}"
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

        # Write DAG file to shared volume
        yaml_str = self._yaml_generator.generate(pipeline)
        dag_code = self._dag_generator.generate(yaml_str)
        dag_file = pathlib.Path(self._dags_path) / f"{pipeline.name}.py"
        dag_file.parent.mkdir(parents=True, exist_ok=True)
        dag_file.write_text(dag_code, encoding="utf-8")
        logger.info(
            "DAG written to %s | pipeline_id=%s | triggered_by=%s",
            dag_file,
            pipeline_id,
            triggered_by,
        )

        await self._orchestrator.trigger_dag(
            pipeline_id=pipeline.id,
            run_id=run.id,
            dag_run_id=dag_run_id,
            pipeline_name=pipeline.name,
        )

        if self._telemetry:
            self._telemetry.record_event(
                "platform.pipeline.triggered",
                {
                    "pipeline_id": pipeline.id,
                    "run_id": run.id,
                    "pipeline_name": pipeline.name,
                },
            )

        return run
