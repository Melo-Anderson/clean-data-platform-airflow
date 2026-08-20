from __future__ import annotations

from app.application.shared.ports.pipeline_validator_port import PipelineValidatorPort
from app.domain.pipelines.validation import ValidationResult


class ValidateHarnessPipelineUseCase:
    def __init__(self, validator: PipelineValidatorPort | None = None) -> None:
        self._validator = validator

    async def execute(
        self,
        pipeline_yaml: str,
        pipeline_type: str = "ingestion",
        endpoint_type: str = "relational",
    ) -> ValidationResult:
        if self._validator is None:
            raise RuntimeError("PipelineValidatorPort is required to validate pipeline")
        return self._validator.validate(pipeline_yaml, pipeline_type, endpoint_type)
