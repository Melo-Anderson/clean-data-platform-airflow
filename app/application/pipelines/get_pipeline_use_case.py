# app/application/pipelines/get_pipeline_use_case.py
from __future__ import annotations

from app.application.unit_of_work import UnitOfWork
from app.domain.pipelines.pipeline import Pipeline
from app.domain.shared.exceptions import PlatformNotFoundError


class GetPipelineUseCase:
    """Fetch a single pipeline by ID. Raises PlatformNotFoundError if absent."""

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def execute(self, pipeline_id: str) -> Pipeline:
        async with self._uow:
            pipeline = await self._uow.pipelines.find_by_id(pipeline_id)
        if pipeline is None:
            raise PlatformNotFoundError(f"Pipeline not found: {pipeline_id}")
        return pipeline
