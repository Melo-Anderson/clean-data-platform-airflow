from __future__ import annotations

from datetime import date

from app.application.shared.ports.backfill_port import BackfillPort
from app.application.unit_of_work import UnitOfWork
from app.domain.shared.exceptions import PlatformNotFoundError


class TriggerBackfillUseCase:
    def __init__(self, uow: UnitOfWork, backfill_adapter: BackfillPort) -> None:
        self._uow = uow
        self._backfill_adapter = backfill_adapter

    async def execute(self, pipeline_id: str, from_date: date, to_date: date) -> str:
        async with self._uow:
            pipeline = await self._uow.pipelines.find_by_id(pipeline_id)
        if pipeline is None:
            raise PlatformNotFoundError(f"Pipeline not found: {pipeline_id}")
        return await self._backfill_adapter.create_backfill(
            dag_id=pipeline.name,
            from_date=from_date,
            to_date=to_date,
        )
