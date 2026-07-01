from __future__ import annotations

from app.domain.pipelines.pipeline import Pipeline
from app.domain.pipelines.pipeline_repository import PipelineRepository


class PipelineNotFoundError(Exception):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__(f"Pipeline not found: {pipeline_id}")
        self.pipeline_id = pipeline_id


class PipelineService:
    def __init__(self, repo: PipelineRepository) -> None:
        self._repo = repo

    async def register(self, pipeline: Pipeline) -> Pipeline:
        # Business logic validation: sensors exceeding execution timeout
        for source in pipeline.source_objects:
            if (
                source.sensor
                and source.sensor.timeout_minutes > pipeline.airflow.execution_timeout_minutes
            ):
                raise ValueError(
                    f"sensor timeout ({source.sensor.timeout_minutes}m) cannot exceed "
                    f"pipeline execution timeout ({pipeline.airflow.execution_timeout_minutes}m)"
                )
        return await self._repo.save(pipeline)
