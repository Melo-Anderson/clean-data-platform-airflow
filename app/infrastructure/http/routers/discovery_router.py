from __future__ import annotations

from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends, status

from app.application.discovery.approve_drift_use_case import ApproveDriftUseCase
from app.application.discovery.get_discovery_snapshot_use_case import (
    GetDiscoverySnapshotUseCase,
)
from app.application.discovery.run_discovery_use_case import RunDiscoveryUseCase
from app.auth.current_user import CurrentUser
from app.auth.dependencies import require_permission
from app.domain.discovery.drift_approval_decision import DriftApprovalDecision
from app.domain.shared.exceptions import PlatformValidationError
from app.infrastructure.http.audit_helper import write_audit_log_task
from app.infrastructure.http.dependencies import (
    get_approve_drift_use_case,
    get_discovery_snapshot_use_case,
    get_run_discovery_use_case,
)
from app.infrastructure.http.schemas.discovery_schemas import (
    DiscoveryRunResponse,
    DriftApprovalResponse,
    DriftDecisionRequest,
    TriggerDiscoveryRequest,
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
    current_user: CurrentUser = Depends(require_permission("catalog:view")),
    use_case: RunDiscoveryUseCase = Depends(get_run_discovery_use_case),
) -> DiscoveryRunResponse:
    """
    Triggers a DiscoveryRun for a given asset.
    Orchestrates extraction, diffing, self-healing, and approval generation.
    """
    run = await use_case.execute(asset_name=asset_name, triggered_by=body.triggered_by)

    background_tasks.add_task(
        write_audit_log_task,
        actor_id=current_user.id,
        actor_email=str(current_user.email),
        event_type="discovery.run_triggered",
        entity_type="DiscoveryRun",
        entity_id=run.id,
        payload={"asset_id": run.asset_id},
        description="Discovery run triggered manually",
    )

    return DiscoveryRunResponse.model_validate(run)


@router.post("/approvals/{approval_id}/decision", response_model=DriftApprovalResponse)
async def decide_drift_approval(
    approval_id: str,
    body: DriftDecisionRequest,
    background_tasks: BackgroundTasks,
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


@router.get("/assets/{asset_name}/snapshot")
async def get_latest_discovery_snapshot_for_asset(
    asset_name: str,
    use_case: GetDiscoverySnapshotUseCase = Depends(get_discovery_snapshot_use_case),
) -> dict[str, Any]:
    """Get latest discovered schema snapshot (data objects and elements) for an asset."""
    return await use_case.execute(asset_name=asset_name)
