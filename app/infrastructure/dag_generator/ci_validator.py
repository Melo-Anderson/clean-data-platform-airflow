from __future__ import annotations

from typing import Any

import yaml
from croniter import croniter
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from app.domain.pipelines.compute_engine import ComputeEngine
from app.domain.pipelines.pipeline_type import PipelineType
from app.domain.pipelines.schedule_mode import ScheduleMode

VALID_COMPUTE_ENGINES = {e.value for e in ComputeEngine}
VALID_PIPELINE_TYPES = {e.value for e in PipelineType}
VALID_SCHEDULE_MODES = {e.value for e in ScheduleMode}
VALID_QUALITY_TYPES = {
    "row_count_min",
    "row_count_max",
    "not_null",
    "unique",
    "value_range",
    "custom_sql",
}


class SensorSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    query: str | None = None
    timeout_minutes: int = 60
    poke_interval_seconds: int = 30


class SourceObjectSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    object_name: str = "default_object"
    sensor: SensorSchema | None = None


class SourceSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    asset_name: str | None = None
    objects: list[SourceObjectSchema] = Field(default_factory=list)


class AirflowSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    retries: int = Field(ge=0, default=3)
    retry_delay_minutes: int = Field(gt=0, default=5)
    execution_timeout_minutes: int = Field(gt=0, default=120)
    sla_minutes: int = Field(gt=0, default=90)
    pool: str = "default_pool"
    tags: list[str] = Field(default_factory=list)


class ScheduleSection(BaseModel):
    model_config = ConfigDict(extra="ignore")
    mode: str = "cron"
    cron: str | None = None

    @field_validator("mode")
    @classmethod
    def validate_mode(cls, v: str) -> str:
        if v not in VALID_SCHEDULE_MODES:
            raise ValueError(f"Invalid schedule.mode: {v!r}. Valid: {sorted(VALID_SCHEDULE_MODES)}")
        return v

    @field_validator("cron")
    @classmethod
    def validate_cron(cls, v: str | None) -> str | None:
        if v is not None and not croniter.is_valid(v):
            raise ValueError(f"Invalid cron expression: {v!r}")
        return v


class ComputeSection(BaseModel):
    model_config = ConfigDict(extra="ignore")
    engine: str = "default"
    staging_bucket: str | None = None

    @field_validator("engine")
    @classmethod
    def validate_engine(cls, v: str) -> str:
        if v not in VALID_COMPUTE_ENGINES:
            raise ValueError(
                f"Invalid compute.engine: {v!r}. Valid: {sorted(VALID_COMPUTE_ENGINES)}"
            )
        return v


class PipelineSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = "unassigned"
    name: str = "unnamed"
    type: str = "ingestion"
    owner: str = Field(default="owner@platform.internal", min_length=1)
    schedule: ScheduleSection = Field(default_factory=ScheduleSection)
    airflow: AirflowSchema = Field(default_factory=AirflowSchema)
    source: SourceSchema = Field(default_factory=SourceSchema)
    compute: ComputeSection = Field(default_factory=ComputeSection)
    quality: dict[str, Any] = Field(default_factory=dict)

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        if v not in VALID_PIPELINE_TYPES:
            raise ValueError(f"Invalid type: {v!r}. Valid: {sorted(VALID_PIPELINE_TYPES)}")
        return v

    @field_validator("owner")
    @classmethod
    def validate_owner(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Owner must not be empty")
        return v

    @model_validator(mode="after")
    def validate_transformation_engine(self) -> PipelineSchema:
        if self.type == "transformation" and self.compute.engine in ("default", ""):
            raise ValueError(
                "compute.engine 'default' is not allowed for transformation pipelines. "
                f"Must be one of: {sorted(e.value for e in ComputeEngine if e != ComputeEngine.DEFAULT)}"
            )
        return self


class RootSchema(BaseModel):
    pipeline: PipelineSchema


class CiValidator:
    """Validates Pipeline YAML configurations statically using Pydantic."""

    def validate_yaml(self, yaml_content: str) -> list[str]:
        """Validates a YAML string and returns a list of error messages. Empty if valid."""
        try:
            doc = yaml.safe_load(yaml_content)
        except yaml.YAMLError as exc:
            return [f"Invalid YAML: {exc}"]

        if not isinstance(doc, dict) or "pipeline" not in doc:
            return ["YAML must contain a 'pipeline' root key."]

        try:
            parsed = RootSchema.model_validate(doc)
        except ValidationError as exc:
            return [f"{err['loc']}: {err['msg']}" for err in exc.errors()]

        return self._validate_sensor_timeout(parsed)

    def _validate_sensor_timeout(self, parsed: RootSchema) -> list[str]:
        errors = []
        exec_timeout = parsed.pipeline.airflow.execution_timeout_minutes
        for obj in parsed.pipeline.source.objects:
            if obj.sensor and obj.sensor.query and obj.sensor.timeout_minutes > exec_timeout:
                errors.append(
                    f"sensor.timeout_minutes ({obj.sensor.timeout_minutes}) > "
                    f"execution_timeout_minutes ({exec_timeout}) "
                    f"for object_name='{obj.object_name}'"
                )
        return errors
