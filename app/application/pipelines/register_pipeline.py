from __future__ import annotations

import pathlib
import uuid
from typing import Any

from app.application.pipelines.commands import RegisterPipelineCommand
from app.application.shared.ports.dwh_provisioner_port import DwhProvisionerPort
from app.application.shared.ports.generator_ports import DagGeneratorPort, YamlGeneratorPort
from app.application.unit_of_work import UnitOfWork
from app.domain.objects.data_object import DataObject
from app.domain.objects.object_type import ObjectType
from app.domain.pipelines.airflow_config import AirflowConfig
from app.domain.pipelines.compute_config import ComputeConfig
from app.domain.pipelines.compute_engine import ComputeEngine
from app.domain.pipelines.destination_object_config import DestinationObjectConfig
from app.domain.pipelines.extraction_config import ExtractionConfig
from app.domain.pipelines.load_strategy import LoadStrategy
from app.domain.pipelines.pipeline import Pipeline
from app.domain.pipelines.pipeline_dependency import PipelineDependency
from app.domain.pipelines.pipeline_type import PipelineType
from app.domain.pipelines.quality_rule import QualityRule
from app.domain.pipelines.quality_rule_type import QualityRuleType
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
    ) -> None:
        self._uow = uow
        self._dwh_provisioner = dwh_provisioner
        self._dags_path = pathlib.Path(dags_path)
        self._yaml_generator = yaml_generator
        self._dag_generator = dag_generator

    async def execute(self, command: RegisterPipelineCommand) -> Pipeline:
        src_asset = command.source_asset_name
        dest_asset = command.destination_asset_name

        if command.cron_schedule:
            sched_cfg = ScheduleConfig(
                mode=ScheduleMode.CRON,
                cron_schedule=CronSchedule(command.cron_schedule),
            )
        else:
            dep_id = src_asset or "upstream_asset"
            sched_cfg = ScheduleConfig(
                mode=ScheduleMode.TRIGGER,
                cron_schedule=None,
                depends_on=(PipelineDependency(pipeline_id=dep_id),),
            )

        pipeline = Pipeline(
            id=str(uuid.uuid4()),
            name=command.name,
            type=PipelineType(command.pipeline_type),
            owner=EmailAddress(command.owner_email),
            schedule=sched_cfg,
            source_asset_name=src_asset,
            destination_asset_name=dest_asset,
            destination_objects=_parse_destination_objects(command.destination_objects or []),
            source_objects=_parse_source_objects(command.source_objects or []),
            compute=_parse_compute(command.compute or {}),
            quality_rules=_parse_quality_rules(command.quality_rules or []),
            airflow=_parse_airflow_config(command.airflow_config or {}),
            schema_version="1.0",
        )

        async with self._uow:
            existing = await self._uow.pipelines.find_by_name(command.name)
            if existing is not None:
                raise PlatformValidationError(
                    f"Pipeline with name '{command.name}' already exists."
                )
            pipeline = await self._uow.pipelines.save(pipeline)

            if dest_asset and command.destination_objects:
                dataset_name = dest_asset

                if self._dwh_provisioner:
                    await self._dwh_provisioner.ensure_dataset_exists(
                        dataset_id=dataset_name,
                        description="",
                        labels={},
                    )

                dest_asset_entity = await self._uow.assets.find_by_name(dest_asset)
                if not dest_asset_entity:
                    dest_asset_entity = await self._uow.assets.find_by_id(dest_asset)

                for obj_cfg in command.destination_objects:
                    obj_name = obj_cfg["object_name"]
                    if not obj_name:
                        continue
                    create_if_not_exists = obj_cfg.get("create_if_not_exists", True)
                    if not create_if_not_exists:
                        continue

                    if dest_asset_entity:
                        existing_objs = await self._uow.objects.find_by_asset_id(
                            dest_asset_entity.id
                        )
                        if not any(o.name == obj_name for o in existing_objs):
                            new_obj = DataObject(
                                id=str(uuid.uuid4()),
                                asset_id=dest_asset_entity.id,
                                name=obj_name,
                                type=ObjectType.TABLE,
                                description=f"Auto-provisioned for pipeline '{command.name}'",
                            )
                            await self._uow.objects.save(new_obj)

                    if self._dwh_provisioner:
                        await self._dwh_provisioner.ensure_table_exists(
                            dataset_id=dataset_name,
                            table_id=obj_name,
                            description=f"Auto-provisioned for pipeline '{command.name}'",
                            labels={"managed_by": "clean_data_platform", "pipeline": command.name},
                            schema_fields=obj_cfg.get("schema_fields"),
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


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _parse_destination_objects(raw: list[dict]) -> list[DestinationObjectConfig]:
    return [
        DestinationObjectConfig(
            object_name=item["object_name"],
            create_if_not_exists=item.get("create_if_not_exists", True),
        )
        for item in raw
    ]


def _parse_source_objects(raw: list[dict]) -> list[ExtractionConfig]:
    return [
        ExtractionConfig(
            object_name=item["object_name"],
            load_strategy=LoadStrategy(item.get("load_strategy", "full_load")),
            watermark_column=item.get("watermark_column"),
            page_size=int(item.get("page_size", 1000)),
            partition_column=item.get("partition_column"),
            compression=item.get("compression", "snappy"),
            encoding=item.get("encoding", "utf-8"),
            extraction_query=item.get("extraction_query"),
            credential_ref=item.get("credential_ref"),
        )
        for item in raw
    ]


def _parse_compute(raw: dict) -> ComputeConfig:
    raw_cfg = raw.get("config")
    cfg: dict[str, Any] = raw_cfg if isinstance(raw_cfg, dict) else raw
    return ComputeConfig(
        engine=ComputeEngine(raw.get("engine", ComputeEngine.DEFAULT.value)),
        staging_bucket=str(raw.get("staging_bucket") or cfg.get("staging_bucket", "")),
        select=str(raw.get("select") or cfg.get("select", "")),
        num_workers=int(cfg.get("num_workers", 1)),
        machine_type=str(cfg.get("machine_type", "n1-standard-2")),
        source_type=str(cfg.get("source_type", "")),
        credential_ref=str(cfg.get("credential_ref", "")),
        driver=str(cfg.get("driver", "")),
    )


def _parse_quality_rules(raw: list[dict]) -> list[QualityRule]:
    return [
        QualityRule(
            type=QualityRuleType(item["type"]),
            column=item.get("column"),
            value=item.get("value"),
        )
        for item in raw
    ]


def _parse_airflow_config(raw: dict) -> AirflowConfig:
    return AirflowConfig(
        retries=int(raw.get("retries", 3)),
        retry_delay_minutes=int(raw.get("retry_delay_minutes", 5)),
        execution_timeout_minutes=int(raw.get("execution_timeout_minutes", 120)),
        sla_minutes=int(raw.get("sla_minutes", 90)),
        tags=tuple(raw.get("tags", [])),
        pool=raw.get("pool", "default_pool"),
    )


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
