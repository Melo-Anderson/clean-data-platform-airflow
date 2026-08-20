from __future__ import annotations

from typing import Protocol, runtime_checkable

from app.domain.pipelines.validation import ValidationResult


@runtime_checkable
class PipelineValidatorPort(Protocol):
    """Port for validating pipeline YAML definitions."""

    def validate(
        self,
        pipeline_yaml: str,
        pipeline_type: str = "ingestion",
        endpoint_type: str = "relational",
    ) -> ValidationResult: ...
