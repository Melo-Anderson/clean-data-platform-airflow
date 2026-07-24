from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, Depends, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.application.pipelines.register_pipeline import RegisterPipelineUseCase
from app.application.pipelines.report_pipeline_run_use_case import ReportPipelineRunUseCase
from app.application.pipelines.trigger_pipeline_run import TriggerPipelineRunUseCase
from app.auth.current_user import CurrentUser
from app.auth.dependencies import require_permission
from app.config import get_settings
from app.domain.shared.exceptions import PlatformNotFoundError, PlatformValidationError
from app.infrastructure.http.audit_helper import write_audit_log_task
from app.infrastructure.http.rate_limiter import limiter
from app.infrastructure.http.schemas.pipeline_schemas import (
    CreatePipelineRequest,
    PipelineResponse,
    PipelineRunResponse,
    QualityGateReportRequest,
    QualityGateReportResponse,
    TriggerRunRequest,
)
from app.infrastructure.persistence.database import get_db, get_session_factory
from app.infrastructure.persistence.sql_unit_of_work import SqlUnitOfWork

router = APIRouter(prefix="/pipelines", tags=["Pipelines"])
settings = get_settings()


@router.post("/", response_model=PipelineResponse, status_code=status.HTTP_201_CREATED)
async def register_pipeline(
    body: CreatePipelineRequest,
    background_tasks: BackgroundTasks,
    current_user: CurrentUser = Depends(require_permission("pipeline:create")),
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
            destination_asset_id=body.destination_asset_id or "",
        )
    except ValueError as exc:
        raise PlatformValidationError(str(exc)) from exc

    background_tasks.add_task(
        write_audit_log_task,
        actor_id=current_user.id,
        actor_email=str(current_user.email),
        event_type="pipeline.created",
        entity_type="Pipeline",
        entity_id=pipeline.id,
        payload={"name": pipeline.name},
        description="Pipeline created via API",
    )

    return PipelineResponse(
        id=pipeline.id,
        name=pipeline.name,
        pipeline_type=pipeline.type.value,
        owner_email=pipeline.owner.value,
        source_asset_id=pipeline.source_asset_id,
        cron_schedule=pipeline.schedule.cron_schedule.expression
        if pipeline.schedule.cron_schedule
        else None,
    )


@router.get("/{pipeline_id}", response_model=PipelineResponse)
async def get_pipeline(
    pipeline_id: str,
    session: AsyncSession = Depends(get_db),
    _: CurrentUser = Depends(require_permission("pipeline:view")),
) -> PipelineResponse:
    from app.infrastructure.persistence.repositories.sql_pipeline_repository import (
        SqlPipelineRepository,
    )

    repo = SqlPipelineRepository(session)
    pipeline = await repo.find_by_id(pipeline_id)
    if pipeline is None:
        raise PlatformNotFoundError(f"Pipeline not found: {pipeline_id}")
    return PipelineResponse(
        id=pipeline.id,
        name=pipeline.name,
        pipeline_type=pipeline.type.value,
        owner_email=pipeline.owner.value,
        source_asset_id=pipeline.source_asset_id,
        cron_schedule=pipeline.schedule.cron_schedule.expression
        if pipeline.schedule.cron_schedule
        else None,
    )


@router.post(
    "/{pipeline_id}/run", response_model=PipelineRunResponse, status_code=status.HTTP_201_CREATED
)
@limiter.limit(settings.rate_limit_write)
async def trigger_pipeline_run(
    request: Request,
    pipeline_id: str,
    body: TriggerRunRequest,
    background_tasks: BackgroundTasks,
    current_user: CurrentUser = Depends(require_permission("pipeline:trigger")),
) -> PipelineRunResponse:
    uow = SqlUnitOfWork(get_session_factory())
    from app.infrastructure.adapters.orchestration.airflow_orchestrator_adapter import (
        AirflowOrchestratorAdapter,
    )

    orchestrator = AirflowOrchestratorAdapter(
        airflow_url=settings.airflow_url,
        username=settings.airflow_username,
        password=settings.airflow_password,
    )
    use_case = TriggerPipelineRunUseCase(uow=uow, orchestrator=orchestrator)
    try:
        run = await use_case.execute(pipeline_id=pipeline_id, triggered_by=body.triggered_by)
    except ValueError as exc:
        raise PlatformNotFoundError(str(exc)) from exc

    background_tasks.add_task(
        write_audit_log_task,
        actor_id=current_user.id,
        actor_email=str(current_user.email),
        event_type="pipeline.run_triggered",
        entity_type="PipelineRun",
        entity_id=run.id,
        payload={"dag_run_id": run.dag_run_id},
        description="Pipeline run triggered manually",
    )

    return PipelineRunResponse(
        id=run.id,
        pipeline_id=run.pipeline_id,
        pipeline_name=run.pipeline_name,
        dag_run_id=run.dag_run_id,
        status=run.status.value,
    )


@router.post(
    "/{pipeline_id}/runs/{run_id}/quality-gate",
    response_model=QualityGateReportResponse,
    status_code=status.HTTP_200_OK,
)
async def report_quality_gate(
    pipeline_id: str,
    run_id: str,
    body: QualityGateReportRequest,
    background_tasks: BackgroundTasks,
) -> QualityGateReportResponse:
    uow = SqlUnitOfWork(get_session_factory())
    use_case = ReportPipelineRunUseCase(uow=uow)
    try:
        run = await use_case.execute(run_id=run_id, metrics=body.metrics)
    except ValueError as exc:
        raise PlatformNotFoundError(str(exc)) from exc

    background_tasks.add_task(
        write_audit_log_task,
        actor_id="airflow_worker",
        actor_email="worker@airflow.apache.org",
        event_type="pipeline.run_completed",
        entity_type="PipelineRun",
        entity_id=run.id,
        payload={"status": run.status.value, "violations": run.quality_violations or []},
        description=f"Pipeline run completed with status: {run.status.value}",
    )

    return QualityGateReportResponse(
        run_id=run.id,
        status=run.status.value,
        violations=run.quality_violations or [],
    )
