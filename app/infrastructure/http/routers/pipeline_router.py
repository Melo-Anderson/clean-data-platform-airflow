from __future__ import annotations

from fastapi import APIRouter, HTTPException, status, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.application.pipelines.register_pipeline import RegisterPipelineUseCase
from app.application.pipelines.trigger_pipeline_run import TriggerPipelineRunUseCase
from app.auth.current_user import CurrentUser
from app.auth.dependencies import get_current_user, require_role
from app.auth.role import Role
from app.infrastructure.adapters.orchestration.logging_orchestrator_adapter import (
    LoggingOrchestratorAdapter,
)
from app.infrastructure.http.schemas.pipeline_schemas import (
    CreatePipelineRequest,
    PipelineResponse,
    PipelineRunResponse,
    TriggerRunRequest,
)
from app.infrastructure.persistence.database import get_db, get_session_factory
from app.infrastructure.persistence.sql_unit_of_work import SqlUnitOfWork

router = APIRouter(prefix="/pipelines", tags=["Pipelines"])


@router.post("/", response_model=PipelineResponse, status_code=status.HTTP_201_CREATED)
async def register_pipeline(
    body: CreatePipelineRequest,
    _: CurrentUser = Depends(require_role(Role.PO_PM, Role.ANALYTICS_ENGINEER)),
) -> PipelineResponse:
    uow = SqlUnitOfWork(get_session_factory())
    use_case = RegisterPipelineUseCase(uow=uow)
    try:
        pipeline = await use_case.execute(
            name=body.name,
            pipeline_type=body.pipeline_type,
            owner_email=body.owner_email,
            source_asset_id=body.source_asset_id,
            cron_schedule=body.cron_schedule,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return PipelineResponse(
        id=pipeline.id,
        name=pipeline.name,
        pipeline_type=pipeline.type.value,
        owner_email=pipeline.owner.value,
        source_asset_id=pipeline.source_asset_id,
        cron_schedule=pipeline.schedule.cron_schedule.expression,
    )


@router.get("/{pipeline_id}", response_model=PipelineResponse)
async def get_pipeline(
    pipeline_id: str,
    session: AsyncSession = Depends(get_db),
    _: CurrentUser = Depends(get_current_user),
) -> PipelineResponse:
    from app.infrastructure.persistence.repositories.sql_pipeline_repository import SqlPipelineRepository
    repo = SqlPipelineRepository(session)
    pipeline = await repo.find_by_id(pipeline_id)
    if pipeline is None:
        raise HTTPException(status_code=404, detail=f"Pipeline not found: {pipeline_id}")
    return PipelineResponse(
        id=pipeline.id,
        name=pipeline.name,
        pipeline_type=pipeline.type.value,
        owner_email=pipeline.owner.value,
        source_asset_id=pipeline.source_asset_id,
        cron_schedule=pipeline.schedule.cron_schedule.expression,
    )


@router.post("/{pipeline_id}/run", response_model=PipelineRunResponse, status_code=status.HTTP_201_CREATED)
async def trigger_pipeline_run(
    pipeline_id: str,
    body: TriggerRunRequest,
    _: CurrentUser = Depends(require_role(Role.PO_PM, Role.ANALYTICS_ENGINEER, Role.SRE)),
) -> PipelineRunResponse:
    uow = SqlUnitOfWork(get_session_factory())
    orchestrator = LoggingOrchestratorAdapter()
    use_case = TriggerPipelineRunUseCase(uow=uow, orchestrator=orchestrator)
    try:
        run = await use_case.execute(pipeline_id=pipeline_id, triggered_by=body.triggered_by)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return PipelineRunResponse(
        id=run.id,
        pipeline_id=run.pipeline_id,
        pipeline_name=run.pipeline_name,
        dag_run_id=run.dag_run_id,
        status=run.status.value,
    )
