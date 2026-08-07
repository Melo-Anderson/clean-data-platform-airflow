from __future__ import annotations

from app.application.pipelines.services.pipeline_validator import PipelineValidator
from app.infrastructure.http.schemas.harness_schemas import (
    ValidationErrorDetail,
    ValidationResponse,
)


class ValidateHarnessPipelineUseCase:
    def __init__(self, validator: PipelineValidator | None = None) -> None:
        self._validator = validator or PipelineValidator()

    async def execute(
        self,
        pipeline_yaml: str,
        pipeline_type: str = "ingestion",
        endpoint_type: str = "relational",
    ) -> ValidationResponse:
        domain_result = self._validator.validate(pipeline_yaml, pipeline_type, endpoint_type)
        return ValidationResponse(
            is_valid=domain_result.is_valid,
            errors=[
                ValidationErrorDetail(
                    json_pointer=err.json_pointer,
                    error_code=err.error_code,
                    message=err.message,
                    suggestion=err.suggestion,
                )
                for err in domain_result.errors
            ],
        )
