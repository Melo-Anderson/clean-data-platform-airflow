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
    ValidationRequest,
    ValidationResponse,
)
from app.infrastructure.persistence.database import get_session_factory
from app.infrastructure.persistence.sql_unit_of_work import SqlUnitOfWork

router = APIRouter(prefix="/harness", tags=["Harness"])
settings = get_settings()


@router.post("/validate", response_model=ValidationResponse)
@limiter.limit(settings.rate_limit_write)
async def validate_pipeline(request: Request, body: ValidationRequest) -> ValidationResponse:
    use_case = ValidateHarnessPipelineUseCase()
    return await use_case.execute(
        pipeline_yaml=body.pipeline_yaml,
        pipeline_type=body.pipeline_type,
    )


@router.get("/schema", response_model=HarnessSchemaResponse)
async def get_schema(type: str = "all") -> HarnessSchemaResponse:
    use_case = GetHarnessSchemaUseCase()
    return await use_case.execute(pipeline_type=type)


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
    use_case = GetHarnessGoldExamplesUseCase(uow=uow)
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
    use_case = GetPipelineYamlUseCase(uow=uow)
    try:
        async with uow:
            result = await use_case.execute(pipeline_id=pipeline_id)
        return PipelineYamlExportResponse(**result)
    except ValueError as exc:
        raise PlatformNotFoundError(str(exc)) from exc
