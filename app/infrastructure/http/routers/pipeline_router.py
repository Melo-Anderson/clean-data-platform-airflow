from __future__ import annotations

import uuid
from datetime import datetime

from fastapi import APIRouter, BackgroundTasks, Depends, Query, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.application.pipelines.register_pipeline import RegisterPipelineUseCase
from app.application.pipelines.report_pipeline_run_use_case import ReportPipelineRunUseCase
from app.application.pipelines.trigger_pipeline_run import TriggerPipelineRunUseCase
from app.auth.current_user import CurrentUser
from app.auth.dependencies import require_permission
from app.config import get_settings
from app.domain.pipelines.quality_gate_evaluator import QualityGateEvaluator
from app.domain.shared.exceptions import PlatformNotFoundError
from app.infrastructure.dag_generator.dag_generator import DagGenerator
from app.infrastructure.dwh_provisioners.dwh_provisioner_factory import get_dwh_provisioner
from app.infrastructure.http.audit_helper import write_audit_log_task
from app.infrastructure.http.rate_limiter import limiter
from app.infrastructure.http.schemas.pipeline_schemas import (
    CreatePipelineRequest,
    FailureNotificationRequest,
    PipelineResponse,
    PipelineRunRecordRequest,
    PipelineRunResponse,
    PipelineRunStatusCheckResponse,
    QualityGateReportRequest,
    QualityGateReportResponse,
    TriggerRunRequest,
)
from app.infrastructure.persistence.database import get_db, get_session_factory
from app.infrastructure.persistence.sql_unit_of_work import SqlUnitOfWork
from app.infrastructure.yaml_generator.pipeline_yaml_generator import PipelineYamlGenerator

router = APIRouter(prefix="/pipelines", tags=["Pipelines"])
settings = get_settings()


@router.post("/", response_model=PipelineResponse, status_code=status.HTTP_201_CREATED)
async def register_pipeline(
    body: CreatePipelineRequest,
    background_tasks: BackgroundTasks,
    current_user: CurrentUser = Depends(require_permission("pipeline:create")),
) -> PipelineResponse:
    uow = SqlUnitOfWork(get_session_factory())
    use_case = RegisterPipelineUseCase(
        uow=uow,
        dwh_provisioner=get_dwh_provisioner(get_settings()),
        dags_path=str(get_settings().resolved_dags_path),
        yaml_generator=PipelineYamlGenerator(),
        dag_generator=DagGenerator(),
    )
    pipeline = await use_case.execute(
        name=body.name,
        pipeline_type=body.pipeline_type,
        owner_email=body.owner_email,
        source_asset=body.source_asset or "",
        cron_schedule=body.cron_schedule,
        destination_asset=body.destination_asset or "",
        destination_objects=body.destination_objects,
        source_objects=[o.model_dump() for o in body.source_objects]
        if body.source_objects
        else None,
        compute=body.compute.model_dump() if body.compute else None,
        quality_rules=[r.model_dump() for r in body.quality_rules] if body.quality_rules else None,
        airflow_config=body.airflow_config.model_dump() if body.airflow_config else None,
    )

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
        source_asset=pipeline.source_asset,
        destination_asset=pipeline.destination_asset,
        cron_schedule=pipeline.schedule.cron_schedule.expression
        if pipeline.schedule.cron_schedule
        else None,
    )


@router.get("", response_model=list[PipelineResponse])
@router.get("/", response_model=list[PipelineResponse], include_in_schema=False)
async def list_pipelines(
    session: AsyncSession = Depends(get_db),
    _: CurrentUser = Depends(require_permission("pipeline:view")),
) -> list[PipelineResponse]:
    from app.infrastructure.persistence.repositories.sql_pipeline_repository import (
        SqlPipelineRepository,
    )

    repo = SqlPipelineRepository(session)
    pipelines = await repo.find_all()
    return [
        PipelineResponse(
            id=p.id,
            name=p.name,
            pipeline_type=p.type.value,
            owner_email=p.owner.value,
            source_asset=p.source_asset,
            destination_asset=p.destination_asset,
            cron_schedule=p.schedule.cron_schedule.expression if p.schedule.cron_schedule else None,
        )
        for p in pipelines
    ]


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
        source_asset=pipeline.source_asset,
        destination_asset=pipeline.destination_asset,
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
    use_case = TriggerPipelineRunUseCase(
        uow=uow,
        orchestrator=orchestrator,
        yaml_generator=PipelineYamlGenerator(),
        dag_generator=DagGenerator(),
        dags_path=settings.dags_path,
    )

    run = await use_case.execute(pipeline_id=pipeline_id, triggered_by=body.triggered_by)

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
    use_case = ReportPipelineRunUseCase(uow=uow, quality_gate=QualityGateEvaluator())
    run = await use_case.execute(run_id=run_id, metrics=body.metrics)

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


@router.post(
    "/{pipeline_id}/runs/record",
    response_model=PipelineRunResponse,
    status_code=status.HTTP_201_CREATED,
)
async def record_pipeline_run(
    pipeline_id: str,
    body: PipelineRunRecordRequest,
    background_tasks: BackgroundTasks,
) -> PipelineRunResponse:
    from app.application.pipelines.record_pipeline_run_use_case import RecordPipelineRunUseCase
    from app.domain.pipelines.pipeline_run_file import PipelineRunFile

    uow = SqlUnitOfWork(get_session_factory())
    use_case = RecordPipelineRunUseCase(uow=uow)

    files = [
        PipelineRunFile(
            id=f.id or str(uuid.uuid4()),
            pipeline_run_id=body.id or "",
            file_path=f.file_path,
            file_name=f.file_name,
            file_size_bytes=f.file_size_bytes,
            mtime=f.mtime,
            hash_md5=f.hash_md5,
            status=f.status,
            processed_at=f.processed_at,
        )
        for f in body.files
    ]

    run = await use_case.execute(
        pipeline_id=pipeline_id,
        pipeline_name=body.pipeline_name,
        run_id=body.id,
        pipeline_type=body.pipeline_type,
        dag_run_id=body.dag_run_id,
        status=body.status,
        started_at=body.started_at,
        finished_at=body.finished_at,
        failed_task=body.failed_task,
        optional_failures=body.optional_failures,
        quality_violations=body.quality_violations,
        metrics=body.metrics,
        sla_minutes=body.sla_minutes,
        sla_breached=body.sla_breached,
        files=files,
    )

    background_tasks.add_task(
        write_audit_log_task,
        actor_id="airflow_worker",
        actor_email="worker@airflow.apache.org",
        event_type="pipeline.run_recorded",
        entity_type="PipelineRun",
        entity_id=run.id,
        payload={"status": run.status.value, "files_count": len(files)},
        description=f"Pipeline run recorded via REST API: {run.status.value}",
    )

    return PipelineRunResponse(
        id=run.id,
        pipeline_id=run.pipeline_id,
        pipeline_name=run.pipeline_name,
        dag_run_id=run.dag_run_id,
        status=run.status.value,
    )


@router.get(
    "/{pipeline_id}/runs/latest",
    response_model=PipelineRunStatusCheckResponse,
    summary="Get latest pipeline run status or check success on date",
)
async def get_latest_pipeline_run_status(
    pipeline_id: str,
    require_same_day: bool = Query(False),
    logical_date: datetime | None = Query(None),
    dependency_type: str = Query("same_day"),
    session: AsyncSession = Depends(get_db),
    _: CurrentUser = Depends(require_permission("pipeline:view")),
) -> PipelineRunStatusCheckResponse:
    from app.infrastructure.persistence.repositories.sql_pipeline_run_repository import (
        SqlPipelineRunRepository,
    )

    repo = SqlPipelineRunRepository(session)
    latest_run = await repo.find_latest_by_pipeline_id(pipeline_id)
    if not latest_run:
        return PipelineRunStatusCheckResponse(
            pipeline_id=pipeline_id,
            success=False,
            status=None,
            logical_date=logical_date,
        )

    is_success = latest_run.status.value.lower() in ("success", "partial")
    if require_same_day and logical_date and latest_run.started_at:
        is_success = is_success and (latest_run.started_at.date() == logical_date.date())

    return PipelineRunStatusCheckResponse(
        pipeline_id=pipeline_id,
        success=is_success,
        status=latest_run.status.value,
        logical_date=logical_date,
    )


@router.post(
    "/{pipeline_id}/notifications/failure",
    status_code=status.HTTP_200_OK,
    summary="Record failure notification for a pipeline run",
)
async def notify_pipeline_failure(
    pipeline_id: str,
    body: FailureNotificationRequest,
    background_tasks: BackgroundTasks,
) -> dict[str, str]:
    background_tasks.add_task(
        write_audit_log_task,
        actor_id="airflow_worker",
        actor_email="worker@airflow.apache.org",
        event_type="pipeline.run_failed_notification",
        entity_type="Pipeline",
        entity_id=pipeline_id,
        payload={"failed_task": body.failed_task, "error_message": body.error_message},
        description=f"Failure notification for task: {body.failed_task}",
    )
    return {"status": "ok", "message": "Failure notification received"}
