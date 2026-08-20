from __future__ import annotations

from app.domain.pipelines.pipeline import Pipeline
from app.domain.pipelines.pipeline_repository import PipelineRepository
from app.domain.shared.exceptions import PlatformNotFoundError

# Backward compatibility alias if needed
PipelineNotFoundError = PlatformNotFoundError


class PipelineService:
    def __init__(self, repo: PipelineRepository) -> None:
        self._repo = repo

    async def register(self, pipeline: Pipeline) -> Pipeline:
        pipeline.validate_invariants()
        return await self._repo.save(pipeline)
