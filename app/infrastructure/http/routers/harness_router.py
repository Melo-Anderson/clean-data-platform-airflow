from __future__ import annotations

from fastapi import APIRouter, Request

from app.application.harness.get_harness_gold_examples import GetHarnessGoldExamplesUseCase
from app.application.harness.get_harness_schema import GetHarnessSchemaUseCase
from app.application.harness.validate_harness_pipeline import ValidateHarnessPipelineUseCase
from app.config import get_settings
from app.infrastructure.http.rate_limiter import limiter
from app.infrastructure.http.schemas.harness_schemas import (
    HarnessSchemaResponse,
    ValidationRequest,
    ValidationResponse,
)

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
async def get_gold_examples(type: str = "all") -> dict[str, str]:
    use_case = GetHarnessGoldExamplesUseCase()
    return await use_case.execute(pipeline_type=type)
