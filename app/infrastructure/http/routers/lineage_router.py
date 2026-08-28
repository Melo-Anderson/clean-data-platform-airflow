from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, Depends, Query, status

from app.application.lineage.get_lineage_graph import GetLineageGraphUseCase
from app.auth.current_user import CurrentUser
from app.auth.dependencies import require_permission
from app.domain.shared.exceptions import PlatformValidationError
from app.infrastructure.http.audit_helper import write_audit_log_task
from app.infrastructure.http.schemas.lineage_schemas import (
    EtlLineageRequest,
    ExportLineageRequest,
    FreshnessUpdateRequest,
    LineageEventResponse,
    LineageGraphResponse,
    LineageNodeSchema,
    RawLineageRequest,
)
from app.infrastructure.persistence.database import get_session_factory
from app.infrastructure.persistence.sql_unit_of_work import SqlUnitOfWork

router = APIRouter(prefix="/lineage", tags=["Lineage"])


@router.get(
    "/trace",
    response_model=LineageGraphResponse,
    summary="Get column-level lineage (upstream/downstream/both)",
)
async def trace_lineage(
    object_id: str = Query(..., description="The DataObject ID to trace"),
    column_name: str = Query(..., description="The column name to trace"),
    direction: str = Query(
        "upstream", description="Direction to trace: upstream | downstream | both"
    ),
    _: CurrentUser = Depends(require_permission("catalog:view")),
) -> LineageGraphResponse:
    """
    Trace column-level lineage. Returns nodes upstream (provenance)
    and/or downstream (impact analysis).
    """
    if direction not in ("upstream", "downstream", "both"):
        raise PlatformValidationError(
            "Invalid direction. Choose 'upstream', 'downstream', or 'both'"
        )

    uow = SqlUnitOfWork(get_session_factory())
    use_case = GetLineageGraphUseCase(uow=uow)

    result = await use_case.execute(
        object_id=object_id,
        column_name=column_name,
        direction=direction,
    )
    return LineageGraphResponse(
        upstream=[LineageNodeSchema(**node) for node in result.get("upstream", [])],
        downstream=[LineageNodeSchema(**node) for node in result.get("downstream", [])],
    )


@router.post(
    "/raw",
    response_model=LineageEventResponse,
    status_code=status.HTTP_200_OK,
    summary="Emit raw ingestion lineage event",
)
async def emit_raw_lineage(
    body: RawLineageRequest,
    background_tasks: BackgroundTasks,
) -> LineageEventResponse:
    background_tasks.add_task(
        write_audit_log_task,
        actor_id="airflow_worker",
        actor_email="worker@airflow.apache.org",
        event_type="lineage.raw_emitted",
        entity_type="Pipeline",
        entity_id=body.pipeline_id,
        payload={
            "source_object_ids": body.source_object_ids,
            "destination_object_ids": body.destination_object_ids,
            "schema_path": body.schema_path,
        },
        description="Raw ingestion lineage recorded",
    )
    return LineageEventResponse(
        status="ok",
        pipeline_id=body.pipeline_id,
        message="Raw lineage recorded successfully",
    )


@router.post(
    "/freshness",
    response_model=LineageEventResponse,
    status_code=status.HTTP_200_OK,
    summary="Update freshness status for destination objects",
)
async def update_freshness_status(
    body: FreshnessUpdateRequest,
    background_tasks: BackgroundTasks,
) -> LineageEventResponse:
    background_tasks.add_task(
        write_audit_log_task,
        actor_id="airflow_worker",
        actor_email="worker@airflow.apache.org",
        event_type="lineage.freshness_updated",
        entity_type="Pipeline",
        entity_id=body.pipeline_id,
        payload={"destination_object_ids": body.destination_object_ids},
        description="Freshness status updated",
    )
    return LineageEventResponse(
        status="ok",
        pipeline_id=body.pipeline_id,
        message="Freshness status updated successfully",
    )


@router.post(
    "/etl",
    response_model=LineageEventResponse,
    status_code=status.HTTP_200_OK,
    summary="Emit ETL transformation lineage event",
)
async def emit_etl_lineage(
    body: EtlLineageRequest,
    background_tasks: BackgroundTasks,
) -> LineageEventResponse:
    background_tasks.add_task(
        write_audit_log_task,
        actor_id="airflow_worker",
        actor_email="worker@airflow.apache.org",
        event_type="lineage.etl_emitted",
        entity_type="Pipeline",
        entity_id=body.pipeline_id,
        payload={
            "transform_ref": body.transform_ref,
            "schema_path": body.schema_path,
        },
        description="ETL lineage recorded",
    )
    return LineageEventResponse(
        status="ok",
        pipeline_id=body.pipeline_id,
        message="ETL lineage recorded successfully",
    )


@router.post(
    "/export",
    response_model=LineageEventResponse,
    status_code=status.HTTP_200_OK,
    summary="Emit export lineage event",
)
async def emit_export_lineage(
    body: ExportLineageRequest,
    background_tasks: BackgroundTasks,
) -> LineageEventResponse:
    background_tasks.add_task(
        write_audit_log_task,
        actor_id="airflow_worker",
        actor_email="worker@airflow.apache.org",
        event_type="lineage.export_emitted",
        entity_type="Pipeline",
        entity_id=body.pipeline_id,
        payload={
            "source_object_ids": body.source_object_ids,
            "destination_object_ids": body.destination_object_ids,
            "schema_path": body.schema_path,
        },
        description="Export lineage recorded",
    )
    return LineageEventResponse(
        status="ok",
        pipeline_id=body.pipeline_id,
        message="Export lineage recorded successfully",
    )
