from __future__ import annotations

import dataclasses
import uuid

from app.domain.pipelines.airflow_config import AirflowConfig
from app.domain.pipelines.compute_config import ComputeConfig
from app.domain.pipelines.compute_engine import ComputeEngine
from app.domain.pipelines.destination_object_config import DestinationObjectConfig
from app.domain.pipelines.discovery_task_config import DiscoveryTaskConfig
from app.domain.pipelines.extraction_config import ExtractionConfig
from app.domain.pipelines.pipeline import Pipeline
from app.domain.pipelines.pipeline_type import PipelineType
from app.domain.pipelines.quality_rule import QualityRule
from app.domain.pipelines.schedule_config import ScheduleConfig
from app.domain.pipelines.schedule_mode import ScheduleMode
from app.domain.pipelines.transform_config import TransformConfig
from app.domain.shared.value_objects import CronSchedule, EmailAddress


class PipelineBuilder:
    """Fluent Builder for constructing Pipeline domain aggregates and YAML specifications."""

    def __init__(self, name: str) -> None:
        self._id: str = f"pipeline-{uuid.uuid4().hex[:8]}"
        self._name: str = name
        self._type: PipelineType = PipelineType.INGESTION
        self._owner: EmailAddress = EmailAddress("pipeline_owner@platform.internal")
        self._schedule: ScheduleConfig = ScheduleConfig(
            mode=ScheduleMode.CRON,
            cron_schedule=CronSchedule("0 0 * * *"),
        )
        self._source_asset: str = ""
        self._source_objects: list[ExtractionConfig] = []
        self._destination_asset: str = ""
        self._destination_objects: list[DestinationObjectConfig] = []
        self._transform: TransformConfig = TransformConfig()
        self._compute: ComputeConfig = ComputeConfig()
        self._quality_rules: list[QualityRule] = []
        self._airflow: AirflowConfig = AirflowConfig()
        self._discovery_task: DiscoveryTaskConfig = DiscoveryTaskConfig()

    def with_id(self, pipeline_id: str) -> PipelineBuilder:
        self._id = pipeline_id
        return self

    def with_type(self, pipeline_type: PipelineType) -> PipelineBuilder:
        self._type = pipeline_type
        return self

    def with_owner(self, owner: EmailAddress) -> PipelineBuilder:
        self._owner = owner
        return self

    def from_asset(self, source_asset_name: str) -> PipelineBuilder:
        self._source_asset = source_asset_name
        return self

    def to_asset(self, destination_asset_name: str) -> PipelineBuilder:
        self._destination_asset = destination_asset_name
        return self

    def with_cron_schedule(self, cron_expression: str) -> PipelineBuilder:
        self._schedule = ScheduleConfig(
            mode=ScheduleMode.CRON,
            cron_schedule=CronSchedule(cron_expression),
        )
        return self

    def with_schedule(self, schedule: ScheduleConfig) -> PipelineBuilder:
        self._schedule = schedule
        return self

    def with_transform(self, transform: TransformConfig) -> PipelineBuilder:
        self._transform = transform
        return self

    def with_compute_engine(
        self,
        engine: ComputeEngine | str,
        staging_bucket: str = "",
        num_workers: int = 1,
        machine_type: str = "n1-standard-2",
        select: str = "",
    ) -> PipelineBuilder:
        engine_val = ComputeEngine(engine) if isinstance(engine, str) else engine
        self._compute = ComputeConfig(
            engine=engine_val,
            staging_bucket=staging_bucket,
            num_workers=num_workers,
            machine_type=machine_type,
            select=select,
        )
        return self

    def with_sla_minutes(self, minutes: int) -> PipelineBuilder:
        self._airflow = dataclasses.replace(self._airflow, sla_minutes=minutes)
        return self

    def with_airflow_config(self, airflow: AirflowConfig) -> PipelineBuilder:
        self._airflow = airflow
        return self

    def with_quality_rule(self, rule: QualityRule) -> PipelineBuilder:
        self._quality_rules.append(rule)
        return self

    def with_quality_rules(self, rules: list[QualityRule]) -> PipelineBuilder:
        self._quality_rules.extend(rules)
        return self

    def add_source_object(self, config: ExtractionConfig) -> PipelineBuilder:
        self._source_objects.append(config)
        return self

    def add_destination_object(self, config: DestinationObjectConfig) -> PipelineBuilder:
        self._destination_objects.append(config)
        return self

    def with_discovery_task(self, discovery_task: DiscoveryTaskConfig) -> PipelineBuilder:
        self._discovery_task = discovery_task
        return self

    def build(self) -> Pipeline:
        return Pipeline(
            id=self._id,
            name=self._name,
            type=self._type,
            owner=self._owner,
            schedule=self._schedule,
            source_asset=self._source_asset,
            source_objects=self._source_objects,
            destination_asset=self._destination_asset,
            destination_objects=self._destination_objects,
            transform=self._transform,
            compute=self._compute,
            quality_rules=self._quality_rules,
            airflow=self._airflow,
            discovery_task=self._discovery_task,
        )
