from __future__ import annotations

from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.application.discovery.approve_drift_use_case import ApproveDriftUseCase
from app.application.discovery.run_discovery_use_case import RunDiscoveryUseCase
from app.auth.current_user import CurrentUser
from app.auth.dependencies import require_permission
from app.domain.discovery.drift_approval_decision import DriftApprovalDecision
from app.domain.shared.exceptions import PlatformNotFoundError, PlatformValidationError
from app.infrastructure.http.audit_helper import write_audit_log_task
from app.infrastructure.http.dependencies import (
    get_approve_drift_use_case,
    get_run_discovery_use_case,
)
from app.infrastructure.http.schemas.discovery_schemas import (
    DiscoveryRunResponse,
    DriftApprovalResponse,
    DriftDecisionRequest,
    TriggerDiscoveryRequest,
)
from app.infrastructure.persistence.database import get_db
from app.infrastructure.persistence.repositories.sql_asset_repository import (
    SqlAssetRepository,
)
from app.infrastructure.persistence.repositories.sql_data_object_repository import (
    SqlDataObjectRepository,
)

router = APIRouter(prefix="/discovery", tags=["Discovery"])


@router.post(
    "/assets/{asset_name}/run",
    response_model=DiscoveryRunResponse,
    status_code=status.HTTP_201_CREATED,
)
async def trigger_discovery_run(
    asset_name: str,
    body: TriggerDiscoveryRequest,
    background_tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_db),
    current_user: CurrentUser = Depends(require_permission("catalog:view")),
    use_case: RunDiscoveryUseCase = Depends(get_run_discovery_use_case),
) -> DiscoveryRunResponse:
    """
    Triggers a DiscoveryRun for a given asset.
    Orchestrates extraction, diffing, self-healing, and approval generation.
    """
    repo = SqlAssetRepository(session=session)
    asset = await repo.find_by_name(asset_name)
    if not asset:
        raise PlatformNotFoundError(f"Asset not found: {asset_name}")

    run = await use_case.execute(asset_id=asset.id, triggered_by=body.triggered_by)

    background_tasks.add_task(
        write_audit_log_task,
        actor_id=current_user.id,
        actor_email=str(current_user.email),
        event_type="discovery.run_triggered",
        entity_type="DiscoveryRun",
        entity_id=run.id,
        payload={"asset_id": asset.id},
        description="Discovery run triggered manually",
    )

    return DiscoveryRunResponse.model_validate(run)


@router.post("/approvals/{approval_id}/decision", response_model=DriftApprovalResponse)
async def decide_drift_approval(
    approval_id: str,
    body: DriftDecisionRequest,
    background_tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_db),
    current_user: CurrentUser = Depends(require_permission("drift:approve")),
    use_case: ApproveDriftUseCase = Depends(get_approve_drift_use_case),
) -> DriftApprovalResponse:
    """
    Approve or reject a pending critical drift.
    PO_PM (Asset Owner) only.
    """
    try:
        decision = DriftApprovalDecision(body.decision.lower())
    except ValueError:
        raise PlatformValidationError("Decision must be 'approved', 'rejected' or 'pending'")

    if decision == DriftApprovalDecision.APPROVED:
        approval = await use_case.approve(approval_id, body.decided_by, body.notes)
    elif decision == DriftApprovalDecision.REJECTED:
        approval = await use_case.reject(approval_id, body.decided_by, body.notes)
    else:
        raise PlatformValidationError("Cannot manually set decision to pending")

    background_tasks.add_task(
        write_audit_log_task,
        actor_id=current_user.id,
        actor_email=str(current_user.email),
        event_type="drift_approval.decided",
        entity_type="DriftApproval",
        entity_id=approval.id,
        payload={"decision": body.decision.lower()},
        description="Drift approval decision made manually",
    )

    return DriftApprovalResponse.model_validate(approval)


def _format_element(elem: Any) -> dict[str, Any]:
    dest_type = elem.destination_type.value if elem.destination_type else "string"
    src_type = elem.source_type.value if elem.source_type else None
    return {
        "name": elem.name,
        "normalized_type": dest_type,
        "source_type": src_type,
        "nullable": elem.nullable,
        "is_primary_key": elem.is_primary_key,
    }


@router.get("/assets/{asset_name}/snapshot")
async def get_latest_discovery_snapshot_for_asset(
    asset_name: str,
    session: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Get latest discovered schema snapshot (data objects and elements) for an asset."""
    asset = await SqlAssetRepository(session).find_by_name(asset_name)
    if not asset:
        raise PlatformNotFoundError(f"Asset not found: {asset_name}")

    objects = await SqlDataObjectRepository(session).find_by_asset_id(asset.id)
    objects_dict = {
        obj.name: {"fields": [_format_element(e) for e in obj.elements]} for obj in objects
    }
    all_fields = [f for obj_data in objects_dict.values() for f in obj_data["fields"]]
    return {
        "asset_id": asset.id,
        "asset_name": asset.name,
        "objects": objects_dict,
        "fields": all_fields,
    }
