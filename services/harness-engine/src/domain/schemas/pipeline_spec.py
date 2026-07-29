"""
Canonical Pydantic v2 contract for the platform pipeline YAML.

Mirrors 100% of PipelineYamlGenerator output. Enforces all domain rules at parse time.
Supports all pipeline types: ingestion | etl | export
"""

from __future__ import annotations

from typing import Literal

from croniter import CroniterBadCronError, croniter  # type: ignore[import-untyped]
from pydantic import BaseModel, Field, field_validator, model_validator

# --- Type aliases ---
ScheduleMode = Literal["cron", "trigger", "trigger_with_gate"]
DependencyType = Literal["dataset", "external_event", "manual"]
LoadStrategy = Literal["full_load", "incremental", "cdc"]
TransformEngine = Literal["dbt", "dataform", "none"]
ComputeEngine = Literal["spark", "dataflow", "default", "rest_api"]
QualityRuleTypeEnum = Literal[
    "not_null", "row_count_min", "unique", "accepted_values", "referential_integrity", "checksum"
]
PipelineTypeEnum = Literal["ingestion", "etl", "export"]
OnCriticalChange = Literal["warn", "fail", "ignore"]


class PipelineDependencySpec(BaseModel):
    pipeline_id: str = Field(min_length=1)
    require_same_day: bool = True
    dependency_type: DependencyType = "dataset"


class ScheduleSpec(BaseModel):
    """
    Schedule config. Rules:
      cron            -> cron required, no depends_on
      trigger         -> depends_on required, no cron
      trigger_with_gate -> both required
    """

    mode: ScheduleMode
    cron: str | None = None
    depends_on: list[PipelineDependencySpec] = Field(default_factory=list)

    @field_validator("cron")
    @classmethod
    def validate_cron_expression(cls, v: str | None) -> str | None:
        if v is None:
            return v
        parts = v.strip().split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron expression {v!r}: expected exactly 5 fields.")
        try:
            croniter(v)
        except CroniterBadCronError as exc:
            raise ValueError(f"Invalid cron expression {v!r}: {exc}") from exc
        return v

    @model_validator(mode="after")
    def validate_schedule_mode_rules(self) -> ScheduleSpec:
        if self.mode == "cron":
            if not self.cron:
                raise ValueError("ScheduleSpec(mode='cron') requires cron_schedule")
            if self.depends_on:
                raise ValueError("ScheduleSpec(mode='cron') must not have depends_on")
        if self.mode == "trigger":
            if not self.depends_on:
                raise ValueError(
                    "ScheduleSpec(mode='trigger') requires at least one depends_on entry"
                )
            if self.cron:
                raise ValueError("ScheduleSpec(mode='trigger') must not have a cron expression")
        if self.mode == "trigger_with_gate":
            if not self.cron:
                raise ValueError("ScheduleSpec(mode='trigger_with_gate') requires cron_schedule")
            if not self.depends_on:
                raise ValueError(
                    "ScheduleSpec(mode='trigger_with_gate') requires at least one depends_on entry"
                )
        return self


class SensorSpec(BaseModel):
    """Pre-extraction readiness sensor. query returns truthy when source is ready."""

    query: str
    timeout_minutes: int = 60
    poke_interval_seconds: int = 60

    @model_validator(mode="after")
    def validate_sensor_fields(self) -> SensorSpec:
        if not self.query.strip():
            raise ValueError("SensorSpec.query cannot be empty")
        if self.timeout_minutes <= 0:
            raise ValueError("SensorSpec.timeout_minutes must be > 0")
        if self.poke_interval_seconds <= 0:
            raise ValueError("SensorSpec.poke_interval_seconds must be > 0")
        return self


class ExtractionSpec(BaseModel):
    object_id: str = Field(min_length=1)
    load_strategy: LoadStrategy = "full_load"
    watermark_column: str | None = None
    page_size: int = Field(default=1000, ge=1)
    partition_column: str | None = None
    compression: str = "snappy"
    encoding: str = "utf-8"
    extraction_query: str | None = None
    sensor: SensorSpec | None = None


class SourceSpec(BaseModel):
    asset_id: str = Field(min_length=1)
    objects: list[ExtractionSpec] = Field(min_length=1)


class DestinationObjectSpec(BaseModel):
    object_id: str = Field(min_length=1)
    create_if_not_exists: bool = True


class DestinationSpec(BaseModel):
    asset_id: str = Field(min_length=1)
    objects: list[DestinationObjectSpec] = Field(min_length=1)


class TransformSpec(BaseModel):
    engine: TransformEngine = "none"
    ref: str | None = None

    @model_validator(mode="after")
    def validate_ref_required(self) -> TransformSpec:
        if self.engine != "none" and not self.ref:
            raise ValueError(f"TransformSpec(engine={self.engine!r}) requires ref")
        return self


class ComputeSpec(BaseModel):
    engine: ComputeEngine = "default"
    num_workers: int = Field(default=1, ge=1)
    machine_type: str = "n1-standard-2"
    staging_bucket: str = Field(min_length=1)


class QualityRuleSpec(BaseModel):
    type: QualityRuleTypeEnum
    column: str | None = None
    value: int | float | None = None


class QualitySpec(BaseModel):
    metrics: list[QualityRuleSpec] = Field(default_factory=list)


class AirflowSpec(BaseModel):
    retries: int = Field(default=3, ge=0)
    retry_delay_minutes: int = Field(default=5, ge=1)
    execution_timeout_minutes: int = Field(default=120, ge=1)
    sla_minutes: int = Field(default=90, ge=1)
    tags: list[str] = Field(default_factory=list)
    pool: str = "default_pool"


class DiscoveryTaskSpec(BaseModel):
    enabled: bool = True
    on_critical_change: OnCriticalChange = "warn"


class PipelineSpec(BaseModel):
    """Full canonical YAML pipeline specification. Single Source of Truth."""

    schema_version: str = "1.0"
    pipeline_id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    type: PipelineTypeEnum
    owner: str
    schedule: ScheduleSpec
    source: SourceSpec
    destination: DestinationSpec
    transform: TransformSpec
    compute: ComputeSpec
    quality: QualitySpec
    airflow: AirflowSpec
    discovery_task: DiscoveryTaskSpec

    @field_validator("owner")
    @classmethod
    def validate_owner_email(cls, v: str) -> str:
        if "@" not in v or not v.strip():
            raise ValueError(f"PipelineSpec.owner must be a valid email address, got: {v!r}")
        return v

    model_config = {"populate_by_name": True}
