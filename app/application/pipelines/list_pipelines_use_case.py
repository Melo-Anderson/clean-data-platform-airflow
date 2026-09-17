# app/application/pipelines/list_pipelines_use_case.py
from __future__ import annotations

from app.application.unit_of_work import UnitOfWork
from app.domain.pipelines.pipeline import Pipeline


class ListPipelinesUseCase:
    """Return all registered pipelines. Read-only — no state mutation."""

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def execute(self) -> list[Pipeline]:
        async with self._uow:
            return await self._uow.pipelines.find_all()
