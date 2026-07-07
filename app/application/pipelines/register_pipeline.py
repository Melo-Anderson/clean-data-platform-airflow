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
        destination_asset_id: str = "",
        destination_objects: list[dict] | None = None,
    ) -> Pipeline:
        from app.domain.objects.data_object import DataObject
        from app.domain.objects.object_type import ObjectType

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
            destination_asset_id=destination_asset_id,
            schema_version="1.0",
        )
        async with self._uow:
            # Check name uniqueness
            existing = await self._uow.pipelines.find_by_name(name)
            if existing is not None:
                raise ValueError(f"Pipeline with name '{name}' already exists.")
            pipeline = await self._uow.pipelines.save(pipeline)

            # Provision destination DataObjects
            if destination_asset_id and destination_objects:
                for obj_cfg in destination_objects:
                    obj_name = obj_cfg["name"]
                    create_if_not_exists = obj_cfg.get("create_if_not_exists", True)
                    if not create_if_not_exists:
                        continue
                    existing_objs = await self._uow.objects.find_by_asset_id(destination_asset_id)
                    if not any(o.name == obj_name for o in existing_objs):
                        new_obj = DataObject(
                            id=str(uuid.uuid4()),
                            asset_id=destination_asset_id,
                            name=obj_name,
                            type=ObjectType.TABLE,
                            description=f"Auto-provisioned for pipeline '{name}'",
                        )
                        await self._uow.objects.save(new_obj)

            await self._uow.commit()
        return pipeline
