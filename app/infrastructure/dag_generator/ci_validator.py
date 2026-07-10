from __future__ import annotations

import yaml
from pydantic import BaseModel, Field, ValidationError


class SensorSchema(BaseModel):
    query: str | None = None
    timeout_minutes: int = 60


class SourceObjectSchema(BaseModel):
    object_id: str
    sensor: SensorSchema | None = None


class SourceSchema(BaseModel):
    objects: list[SourceObjectSchema] = Field(default_factory=list)


class AirflowSchema(BaseModel):
    execution_timeout_minutes: int = 120


class PipelineSchema(BaseModel):
    airflow: AirflowSchema = Field(default_factory=AirflowSchema)
    source: SourceSchema = Field(default_factory=SourceSchema)


class RootSchema(BaseModel):
    pipeline: PipelineSchema


class CiValidator:
    """Validates Pipeline YAML configurations statically using Pydantic."""

    def validate_yaml(self, yaml_content: str) -> list[str]:
        """Validates a YAML string and returns a list of error messages. Empty if valid."""
        try:
            doc = yaml.safe_load(yaml_content)
        except yaml.YAMLError as e:
            return [f"Invalid YAML: {e}"]

        if not isinstance(doc, dict) or "pipeline" not in doc:
            return ["YAML must contain a 'pipeline' root key."]

        try:
            parsed = RootSchema.model_validate(doc)
        except ValidationError as e:
            return [f"Schema validation error: {err['msg']} at {err['loc']}" for err in e.errors()]

        return self._validate_sensor_timeout(parsed)

    def _validate_sensor_timeout(self, parsed: RootSchema) -> list[str]:
        errors = []
        exec_timeout = parsed.pipeline.airflow.execution_timeout_minutes
        for obj in parsed.pipeline.source.objects:
            if obj.sensor and obj.sensor.query and obj.sensor.timeout_minutes > exec_timeout:
                errors.append(
                    f"sensor.timeout_minutes ({obj.sensor.timeout_minutes}) > "
                    f"execution_timeout_minutes ({exec_timeout}) "
                    f"for object_id='{obj.object_id}'"
                )
        return errors
