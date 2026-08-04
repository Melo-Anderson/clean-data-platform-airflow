from __future__ import annotations

import pathlib
import uuid

from app.application.shared.adapters.dwh_provisioner_adapter import DwhProvisionerAdapter
from app.application.unit_of_work import UnitOfWork
from app.domain.pipelines.airflow_config import AirflowConfig
from app.domain.pipelines.compute_config import ComputeConfig
from app.domain.pipelines.compute_engine import ComputeEngine
from app.domain.pipelines.extraction_config import ExtractionConfig
from app.domain.pipelines.load_strategy import LoadStrategy
from app.domain.pipelines.pipeline import Pipeline
from app.domain.pipelines.pipeline_type import PipelineType
from app.domain.pipelines.quality_rule import QualityRule
from app.domain.pipelines.quality_rule_type import QualityRuleType
from app.domain.pipelines.schedule_config import ScheduleConfig
from app.domain.pipelines.schedule_mode import ScheduleMode
from app.domain.shared.value_objects import CronSchedule, EmailAddress
from app.infrastructure.dag_generator.dag_generator import DagGenerator
from app.infrastructure.dwh_provisioners.noop_provisioner import NoOpDwhProvisioner
from app.infrastructure.yaml_generator.pipeline_yaml_generator import PipelineYamlGenerator


class RegisterPipelineUseCase:
    def __init__(
        self,
        uow: UnitOfWork,
        dwh_provisioner: DwhProvisionerAdapter | None = None,
        dags_path: str = "/opt/airflow/dags",
    ) -> None:
        self._uow = uow
        self._dwh_provisioner = dwh_provisioner or NoOpDwhProvisioner()
        self._dags_path = pathlib.Path(dags_path)
        self._yaml_generator = PipelineYamlGenerator()
        self._dag_generator = DagGenerator()

    async def execute(
        self,
        name: str,
        pipeline_type: str,
        owner_email: str,
        source_asset_id: str,
        cron_schedule: str,
        destination_asset_id: str = "",
        destination_objects: list[dict] | None = None,
        source_objects: list[dict] | None = None,
        compute: dict | None = None,
        quality_rules: list[dict] | None = None,
        airflow_config: dict | None = None,
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
            source_objects=_parse_source_objects(source_objects or []),
            compute=_parse_compute(compute or {}),
            quality_rules=_parse_quality_rules(quality_rules or []),
            airflow=_parse_airflow_config(airflow_config or {}),
            schema_version="1.0",
        )

        async with self._uow:
            existing = await self._uow.pipelines.find_by_name(name)
            if existing is not None:
                raise ValueError(f"Pipeline with name '{name}' already exists.")
            pipeline = await self._uow.pipelines.save(pipeline)

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

        _write_dag_file(pipeline, self._dags_path, self._yaml_generator, self._dag_generator)
        return pipeline


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _parse_source_objects(raw: list[dict]) -> list[ExtractionConfig]:
    return [
        ExtractionConfig(
            object_id=item["object_id"],
            load_strategy=LoadStrategy(item.get("load_strategy", "full_load")),
            watermark_column=item.get("watermark_column"),
            page_size=int(item.get("page_size", 1000)),
            partition_column=item.get("partition_column"),
            compression=item.get("compression", "snappy"),
            encoding=item.get("encoding", "utf-8"),
            extraction_query=item.get("extraction_query"),
        )
        for item in raw
    ]


def _parse_compute(raw: dict) -> ComputeConfig:
    return ComputeConfig(
        engine=ComputeEngine(raw.get("engine", ComputeEngine.DEFAULT.value)),
        num_workers=int(raw.get("num_workers", 1)),
        machine_type=raw.get("machine_type", "n1-standard-2"),
        staging_bucket=raw.get("staging_bucket", ""),
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
    yaml_generator: PipelineYamlGenerator,
    dag_generator: DagGenerator,
) -> None:
    dags_path.mkdir(parents=True, exist_ok=True)
    pipeline_yaml = yaml_generator.generate(pipeline)
    dag_code = dag_generator.generate(pipeline_yaml)
    safe_name = pipeline.name.replace(" ", "_").replace("&", "and")
    dag_file = dags_path / f"dag_p_{safe_name}.py"
    dag_file.write_text(dag_code, encoding="utf-8")
