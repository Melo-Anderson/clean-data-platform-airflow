from __future__ import annotations

import uuid

from app.application.unit_of_work import UnitOfWork
from app.domain.pipelines.pipeline import Pipeline
from app.domain.pipelines.pipeline_type import PipelineType
from app.domain.pipelines.schedule_config import ScheduleConfig
from app.domain.pipelines.schedule_mode import ScheduleMode
from app.domain.shared.value_objects import CronSchedule, EmailAddress


class RegisterPipelineUseCase:
    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def execute(
        self,
        name: str,
        pipeline_type: str,
        owner_email: str,
        source_asset_id: str,
        cron_schedule: str,
    ) -> Pipeline:
        pipeline = Pipeline(
            id=str(uuid.uuid4()),
            name=name,
            type=PipelineType(pipeline_type),
            owner=EmailAddress(owner_email),
            schedule=ScheduleConfig(
                mode=ScheduleMode.CRON,
                cron_schedule=CronSchedule(cron_schedule),
            ),
            source_asset_id=source_asset_id,
            schema_version="1.0",
        )
        async with self._uow:
            pipeline = await self._uow.pipelines.save(pipeline)
            await self._uow.commit()
        return pipeline
