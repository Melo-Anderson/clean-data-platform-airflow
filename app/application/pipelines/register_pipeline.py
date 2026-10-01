from __future__ import annotations

import pathlib
import uuid

from app.application.pipelines.commands import RegisterPipelineCommand
from app.application.pipelines.pipeline_normalizer import PipelineNormalizer
from app.application.shared.ports.dwh_provisioner_port import DwhProvisionerPort
from app.application.shared.ports.generator_ports import DagGeneratorPort, YamlGeneratorPort
from app.application.unit_of_work import UnitOfWork
from app.domain.assets.data_asset import DataAsset
from app.domain.objects.data_object import DataObject
from app.domain.objects.object_type import ObjectType
from app.domain.pipelines.destination_object_config import DestinationObjectConfig
from app.domain.pipelines.pipeline import Pipeline
from app.domain.pipelines.pipeline_dependency import PipelineDependency
from app.domain.pipelines.pipeline_type import PipelineType
from app.domain.pipelines.schedule_config import ScheduleConfig
from app.domain.pipelines.schedule_mode import ScheduleMode
from app.domain.shared.exceptions import PlatformValidationError
from app.domain.shared.value_objects import CronSchedule, EmailAddress


class RegisterPipelineUseCase:
    def __init__(
        self,
        uow: UnitOfWork,
        dwh_provisioner: DwhProvisionerPort | None = None,
        dags_path: str = "/opt/airflow/dags",
        yaml_generator: YamlGeneratorPort | None = None,
        dag_generator: DagGeneratorPort | None = None,
        normalizer: PipelineNormalizer | None = None,
    ) -> None:
        self._uow = uow
        self._dwh_provisioner = dwh_provisioner
        self._dags_path = pathlib.Path(dags_path)
        self._yaml_generator = yaml_generator
        self._dag_generator = dag_generator
        self._normalizer = normalizer or PipelineNormalizer()

    async def execute(self, command: RegisterPipelineCommand) -> Pipeline:
        sched_cfg = self._build_schedule_config(command)
        destination_objects = self._normalizer.normalize_destination_objects(
            command.destination_objects or []
        )
        source_objects = self._normalizer.normalize_extraction(command.source_objects or [])

        pipeline = Pipeline(
            id=str(uuid.uuid4()),
            name=command.name,
            type=PipelineType(command.pipeline_type),
            owner=EmailAddress(command.owner_email),
            schedule=sched_cfg,
            source_asset_name=command.source_asset_name,
            destination_asset_name=command.destination_asset_name,
            destination_objects=destination_objects,
            source_objects=source_objects,
            compute=self._normalizer.normalize_compute(command.compute or {}),
            quality_rules=self._normalizer.normalize_quality_rules(command.quality_rules or []),
            airflow=self._normalizer.normalize_airflow(command.airflow_config or {}),
            schema_version="1.0",
        )

        async with self._uow:
            existing = await self._uow.pipelines.find_by_name(command.name)
            if existing is not None:
                raise PlatformValidationError(
                    f"Pipeline with name '{command.name}' already exists."
                )

            pipeline = await self._uow.pipelines.save(pipeline)

            dest_asset_entity = await self._resolve_asset(command.destination_asset_name)
            src_asset_entity = await self._resolve_asset(command.source_asset_name)

            if command.destination_asset_name and destination_objects:
                await self._provision_destination_objects(
                    dataset_name=command.destination_asset_name,
                    dest_asset_entity=dest_asset_entity,
                    pipeline_name=command.name,
                    destination_objects=destination_objects,
                )

            if src_asset_entity:
                await self._link_objects(
                    pipeline_id=pipeline.id,
                    asset_id=src_asset_entity.id,
                    object_names=[o.object_name for o in source_objects],
                    role="source",
                )

            if dest_asset_entity:
                await self._link_objects(
                    pipeline_id=pipeline.id,
                    asset_id=dest_asset_entity.id,
                    object_names=[o.object_name for o in destination_objects],
                    role="destination",
                )

            self._uow.audit_logs.save(
                event_type="pipeline.registered",
                entity_type="Pipeline",
                entity_id=pipeline.id,
                actor_id="system",
                actor_email="system@platform.local",
                payload={"pipeline_type": command.pipeline_type},
                description=f"Pipeline {command.name} registered",
            )
            await self._uow.commit()

        if self._yaml_generator and self._dag_generator:
            _write_dag_file(pipeline, self._dags_path, self._yaml_generator, self._dag_generator)

        return pipeline

    def _build_schedule_config(self, command: RegisterPipelineCommand) -> ScheduleConfig:
        if command.cron_schedule:
            return ScheduleConfig(
                mode=ScheduleMode.CRON,
                cron_schedule=CronSchedule(command.cron_schedule),
            )
        dep_id = command.source_asset_name or "upstream_asset"
        return ScheduleConfig(
            mode=ScheduleMode.TRIGGER,
            cron_schedule=None,
            depends_on=(PipelineDependency(pipeline_id=dep_id),),
        )

    async def _resolve_asset(self, asset_name: str | None) -> DataAsset | None:
        if not asset_name:
            return None
        return await self._uow.assets.find_by_name(asset_name)

    async def _provision_destination_objects(
        self,
        dataset_name: str,
        dest_asset_entity: DataAsset | None,
        pipeline_name: str,
        destination_objects: list[DestinationObjectConfig],
    ) -> None:
        if self._dwh_provisioner:
            await self._dwh_provisioner.ensure_dataset_exists(
                dataset_id=dataset_name,
                description="",
                labels={},
            )

        existing_obj_names: set[str] = set()
        if dest_asset_entity:
            existing_objs = await self._uow.objects.find_by_asset_id(dest_asset_entity.id)
            existing_obj_names = {o.name for o in existing_objs}

        for dst_cfg in destination_objects:
            if not dst_cfg.create_if_not_exists:
                continue

            if dest_asset_entity and dst_cfg.object_name not in existing_obj_names:
                new_obj = DataObject(
                    id=str(uuid.uuid4()),
                    asset_id=dest_asset_entity.id,
                    name=dst_cfg.object_name,
                    type=ObjectType.TABLE,
                    description=f"Auto-provisioned for pipeline '{pipeline_name}'",
                )
                await self._uow.objects.save(new_obj)
                existing_obj_names.add(dst_cfg.object_name)

            if self._dwh_provisioner:
                await self._dwh_provisioner.ensure_table_exists(
                    dataset_id=dataset_name,
                    table_id=dst_cfg.object_name,
                    description=f"Auto-provisioned for pipeline '{pipeline_name}'",
                    labels={"managed_by": "clean_data_platform", "pipeline": pipeline_name},
                    schema_fields=dst_cfg.schema_fields,
                )

    async def _link_objects(
        self,
        pipeline_id: str,
        asset_id: str,
        object_names: list[str],
        role: str,
    ) -> None:
        if not object_names:
            return
        asset_objs = await self._uow.objects.find_by_asset_id(asset_id)
        for name in object_names:
            clean_name = name.split(".")[-1]
            matched = next(
                (o for o in asset_objs if o.name in (name, clean_name)),
                None,
            )
            if matched:
                await self._uow.pipelines.link_object(pipeline_id, matched.id, role)


def _write_dag_file(
    pipeline: Pipeline,
    dags_path: pathlib.Path,
    yaml_generator: YamlGeneratorPort,
    dag_generator: DagGeneratorPort,
) -> None:
    dags_path.mkdir(parents=True, exist_ok=True)
    pipeline_yaml = yaml_generator.generate(pipeline)
    dag_code = dag_generator.generate(pipeline_yaml)
    safe_name = pipeline.name.replace(" ", "_").replace("&", "and")
    dag_file = dags_path / f"dag_p_{safe_name}.py"
    dag_file.write_text(dag_code, encoding="utf-8")
