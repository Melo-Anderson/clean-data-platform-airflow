from __future__ import annotations

import uuid

from app.application.shared.adapters.dwh_provisioner_adapter import DwhProvisionerAdapter
from app.application.unit_of_work import UnitOfWork
from app.domain.pipelines.pipeline import Pipeline
from app.domain.pipelines.pipeline_type import PipelineType
from app.domain.pipelines.schedule_config import ScheduleConfig
from app.domain.pipelines.schedule_mode import ScheduleMode
from app.domain.shared.value_objects import CronSchedule, EmailAddress
from app.infrastructure.dwh_provisioners.noop_provisioner import NoOpDwhProvisioner


class RegisterPipelineUseCase:
    def __init__(
        self, uow: UnitOfWork, dwh_provisioner: DwhProvisionerAdapter | None = None
    ) -> None:
        self._uow = uow
        self._dwh_provisioner = dwh_provisioner or NoOpDwhProvisioner()

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
                dest_asset = await self._uow.assets.find_by_id(destination_asset_id)
                dataset_name = dest_asset.name if dest_asset else destination_asset_id

                await self._dwh_provisioner.ensure_dataset_exists(
                    dataset_id=dataset_name,
                    description="",
                    labels={},
                )

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

                    await self._dwh_provisioner.ensure_table_exists(
                        dataset_id=dataset_name,
                        table_id=obj_name,
                        description=f"Auto-provisioned for pipeline '{name}'",
                        labels={"managed_by": "clean_data_platform", "pipeline": name},
                        schema_fields=obj_cfg.get("schema_fields"),
                    )

            self._uow.audit_logs.save(
                event_type="pipeline.registered",
                entity_type="Pipeline",
                entity_id=pipeline.id,
                actor_id="system",
                actor_email="system@platform.local",
                payload={"pipeline_type": pipeline_type},
                description=f"Pipeline {name} registered",
            )
            await self._uow.commit()
        return pipeline
