from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.application.discovery.approve_drift_use_case import ApproveDriftUseCase
from app.application.discovery.run_discovery_use_case import RunDiscoveryUseCase
from app.auth.current_user import CurrentUser
from app.auth.dependencies import get_current_user, require_role
from app.auth.role import Role
from app.domain.discovery.services.policy_tag_inferrer import PolicyTagInferrer
from app.domain.discovery.services.schema_differ import SchemaDiffer
from app.infrastructure.discovery.discovery_runner_factory import DiscoveryRunnerFactoryImpl
from app.infrastructure.http.schemas.discovery_schemas import (
    DiscoveryRunResponse,
    DriftApprovalResponse,
    DriftDecisionRequest,
    TriggerDiscoveryRequest,
)
from app.domain.discovery.drift_approval_decision import DriftApprovalDecision
from app.infrastructure.persistence.database import get_db, get_session_factory
from app.infrastructure.persistence.sql_unit_of_work import SqlUnitOfWork
from app.config import get_settings
from app.infrastructure.adapters.secrets.secret_manager_factory import get_secret_manager

router = APIRouter(prefix="/discovery", tags=["Discovery"])


@router.post("/assets/{asset_name}/run", response_model=DiscoveryRunResponse, status_code=status.HTTP_201_CREATED)
async def trigger_discovery_run(
    asset_name: str,
    body: TriggerDiscoveryRequest,
    session: AsyncSession = Depends(get_db),
    _: CurrentUser = Depends(require_role(Role.PO_PM, Role.ANALYTICS_ENGINEER, Role.SRE)),
) -> DiscoveryRunResponse:
    """
    Triggers a DiscoveryRun for a given asset.
    Orchestrates extraction, diffing, self-healing, and approval generation.
    """
    uow = SqlUnitOfWork(get_session_factory())
    secret_manager = get_secret_manager(get_settings())
    factory = DiscoveryRunnerFactoryImpl(secret_manager=secret_manager)
    
    use_case = RunDiscoveryUseCase(
        uow=uow,
        runner_factory=factory,
        schema_differ=SchemaDiffer(),
        tag_inferrer=PolicyTagInferrer(),
    )
    
    from app.infrastructure.persistence.repositories.sql_asset_repository import SqlAssetRepository
    repo = SqlAssetRepository(session=session)
    asset = await repo.find_by_name(asset_name)
    if not asset:
        raise HTTPException(status_code=404, detail=f"Asset not found: {asset_name}")
        
    try:
        run = await use_case.execute(asset_id=asset.id, triggered_by=body.triggered_by)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
        
    return DiscoveryRunResponse.model_validate(run)


@router.post("/approvals/{approval_id}/decision", response_model=DriftApprovalResponse)
async def decide_drift_approval(
    approval_id: str,
    body: DriftDecisionRequest,
    _: CurrentUser = Depends(require_role(Role.PO_PM)),
) -> DriftApprovalResponse:
    """
    Approve or reject a pending critical drift.
    PO_PM (Asset Owner) only.
    """
    uow = SqlUnitOfWork(get_session_factory())
    use_case = ApproveDriftUseCase(uow=uow)
    
    try:
        try:
            decision = DriftApprovalDecision(body.decision.lower())
        except ValueError:
            raise HTTPException(status_code=422, detail="Decision must be 'approved', 'rejected' or 'pending'")
            
        if decision == DriftApprovalDecision.APPROVED:
            approval = await use_case.approve(approval_id, body.decided_by, body.notes)
        elif decision == DriftApprovalDecision.REJECTED:
            approval = await use_case.reject(approval_id, body.decided_by, body.notes)
        else:
            raise HTTPException(status_code=422, detail="Cannot manually set decision to pending")
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
        
    return DriftApprovalResponse.model_validate(approval)
