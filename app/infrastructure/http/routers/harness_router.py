from __future__ import annotations

import typing

from fastapi import APIRouter, Request

from app.application.harness.get_harness_gold_examples import GetHarnessGoldExamplesUseCase
from app.application.harness.get_harness_schema import GetHarnessSchemaUseCase
from app.application.harness.get_pipeline_yaml import GetPipelineYamlUseCase
from app.application.harness.validate_harness_pipeline import ValidateHarnessPipelineUseCase
from app.config import get_settings
from app.domain.shared.exceptions import PlatformNotFoundError
from app.infrastructure.http.rate_limiter import limiter
from app.infrastructure.http.schemas.harness_schemas import (
    HarnessSchemaResponse,
    PipelineYamlExportResponse,
    ValidationErrorDetail,
    ValidationRequest,
    ValidationResponse,
)
from app.infrastructure.persistence.database import get_session_factory
from app.infrastructure.persistence.sql_unit_of_work import SqlUnitOfWork
from app.infrastructure.providers.pydantic_schema_provider import PydanticSchemaProvider
from app.infrastructure.validators.pydantic_pipeline_validator import PydanticPipelineValidator
from app.infrastructure.yaml_generator.pipeline_yaml_generator import PipelineYamlGenerator

router = APIRouter(prefix="/harness", tags=["Harness"])
settings = get_settings()


@router.post("/validate", response_model=ValidationResponse)
@limiter.limit(settings.rate_limit_write)
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
) -> dict[str, typing.Any]:
    """Return canonical or real YAML examples for a given pipeline type (unauthenticated)."""
    uow = SqlUnitOfWork(get_session_factory())
    use_case = GetHarnessGoldExamplesUseCase(uow=uow, yaml_generator=PipelineYamlGenerator())
    async with uow:
        return await use_case.execute(
            pipeline_type=type,
            compute_engine=compute_engine,
            transform_engine=transform_engine,
            source_asset_id=source_asset_id,
            limit=limit,
        )


@router.get("/pipelines/{pipeline_id}/yaml", response_model=PipelineYamlExportResponse)
async def get_pipeline_yaml(pipeline_id: str) -> PipelineYamlExportResponse:
    """Return the canonical, self-healed YAML for the given pipeline (unauthenticated)."""
    uow = SqlUnitOfWork(get_session_factory())
    use_case = GetPipelineYamlUseCase(uow=uow, yaml_generator=PipelineYamlGenerator())
    try:
        async with uow:
            result = await use_case.execute(pipeline_id=pipeline_id)
        return PipelineYamlExportResponse(**result)
    except ValueError as exc:
        raise PlatformNotFoundError(str(exc)) from exc
