from __future__ import annotations

import typing

from fastapi import APIRouter, Depends, Request

from app.application.harness.get_harness_gold_examples import GetHarnessGoldExamplesUseCase
from app.application.harness.get_harness_schema import GetHarnessSchemaUseCase
from app.application.harness.get_pipeline_yaml import GetPipelineYamlUseCase
from app.application.harness.validate_harness_pipeline import ValidateHarnessPipelineUseCase
from app.domain.shared.exceptions import PlatformNotFoundError
from app.infrastructure.http.dependencies import (
    get_harness_gold_examples_use_case,
    get_pipeline_yaml_use_case,
)
from app.infrastructure.http.rate_limiter import RATE_LIMIT_WRITE, limiter
from app.infrastructure.http.schemas.harness_schemas import (
    HarnessSchemaResponse,
    PipelineYamlExportResponse,
    ValidationErrorDetail,
    ValidationRequest,
    ValidationResponse,
)
from app.infrastructure.providers.pydantic_schema_provider import PydanticSchemaProvider
from app.infrastructure.validators.pydantic_pipeline_validator import PydanticPipelineValidator

router = APIRouter(prefix="/harness", tags=["Harness"])


@router.post("/validate", response_model=ValidationResponse)
@limiter.limit(RATE_LIMIT_WRITE)
async def validate_pipeline(request: Request, body: ValidationRequest) -> ValidationResponse:
    use_case = ValidateHarnessPipelineUseCase(validator=PydanticPipelineValidator())
    res = await use_case.execute(
        pipeline_yaml=body.pipeline_yaml,
        pipeline_type=body.pipeline_type,
        endpoint_type=body.endpoint_type,
    )
    return ValidationResponse(
        is_valid=res.is_valid,
        errors=[
            ValidationErrorDetail(
                json_pointer=err.json_pointer,
                error_code=err.error_code,
                message=err.message,
                suggestion=err.suggestion,
            )
            for err in res.errors
        ],
    )


@router.get("/schema", response_model=HarnessSchemaResponse)
async def get_schema(
    pipeline_type: str = "ingestion", endpoint_type: str = "relational"
) -> HarnessSchemaResponse:
    use_case = GetHarnessSchemaUseCase(schema_provider=PydanticSchemaProvider())
    res = await use_case.execute(pipeline_type=pipeline_type, endpoint_type=endpoint_type)
    return HarnessSchemaResponse(**res)


@router.get("/gold-examples")
async def get_gold_examples(
    type: str,
    compute_engine: str | None = None,
    transform_engine: str | None = None,
    source_asset_id: str | None = None,
    limit: int = 3,
    use_case: GetHarnessGoldExamplesUseCase = Depends(get_harness_gold_examples_use_case),
) -> dict[str, typing.Any]:
    """Return canonical or real YAML examples for a given pipeline type (unauthenticated)."""
    return await use_case.execute(
        pipeline_type=type,
        compute_engine=compute_engine,
        transform_engine=transform_engine,
        source_asset_id=source_asset_id,
        limit=limit,
    )


@router.get("/pipelines/{pipeline_id}/yaml", response_model=PipelineYamlExportResponse)
async def get_pipeline_yaml(
    pipeline_id: str,
    use_case: GetPipelineYamlUseCase = Depends(get_pipeline_yaml_use_case),
) -> PipelineYamlExportResponse:
    """Return the canonical, self-healed YAML for the given pipeline (unauthenticated)."""
    try:
        result = await use_case.execute(pipeline_id=pipeline_id)
        return PipelineYamlExportResponse(**result)
    except ValueError as exc:
        raise PlatformNotFoundError(str(exc)) from exc
