from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.application.assets.activate_asset import ActivateAssetUseCase
from app.application.assets.register_asset import RegisterAssetUseCase
from app.auth.current_user import CurrentUser
from app.auth.dependencies import get_current_user, require_role
from app.auth.role import Role
from app.domain.assets.asset_service import AssetNotFoundError, InvalidStateTransitionError
from app.config import get_settings
from app.infrastructure.adapters.catalog.catalog_factory import get_catalog_adapter
from app.infrastructure.adapters.notifications.noop_notification_adapter import (
    NoopNotificationAdapter,
)
from app.infrastructure.http.schemas.asset_schemas import (
    AssetCreateRequest,
    AssetResponse,
    AssetUpdateRequest,
    asset_to_response,
)
from app.infrastructure.persistence.database import get_db, get_session_factory
from app.infrastructure.persistence.sql_unit_of_work import SqlUnitOfWork

router = APIRouter()


@router.post("/", response_model=AssetResponse, status_code=status.HTTP_201_CREATED)
async def register_asset(
    body: AssetCreateRequest,
    _: CurrentUser = Depends(require_role(Role.PO_PM, Role.ANALYTICS_ENGINEER)),
) -> AssetResponse:
    """Register a new DataAsset in DRAFT state. No business logic in router."""
    uow = SqlUnitOfWork(get_session_factory())
    use_case = RegisterAssetUseCase(
        uow=uow, catalog=get_catalog_adapter(get_settings()), notifications=NoopNotificationAdapter()
    )
    try:
        asset = await use_case.execute(
            name=body.name,
            description=body.description,
            owner_email=body.owner_email,
            tags=body.tags,
            policy_tags=[t.value for t in body.policy_tags],
            discovery_schedule=body.discovery_schedule,
            discovery_scope_include=body.discovery_scope_include,
            discovery_scope_exclude=body.discovery_scope_exclude,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return asset_to_response(asset)


@router.get("/{asset_name}", response_model=AssetResponse)
async def get_asset(
    asset_name: str,
    session: AsyncSession = Depends(get_db),
    _: CurrentUser = Depends(get_current_user),
) -> AssetResponse:
    """Retrieve a DataAsset by id. Visible to all roles."""
    from app.infrastructure.persistence.repositories.sql_asset_repository import SqlAssetRepository

    repo = SqlAssetRepository(session)
    asset = await repo.find_by_name(asset_name)
    if asset is None:
        raise HTTPException(status_code=404, detail=f"Asset not found: {asset_name!r}")
    return asset_to_response(asset)


@router.post("/{asset_name}/activate", response_model=AssetResponse)
async def activate_asset(
    asset_name: str,
    endpoint_name: str,
    session: AsyncSession = Depends(get_db),
    _: CurrentUser = Depends(require_role(Role.SRE)),
) -> AssetResponse:
    """Transition asset DRAFT → ACTIVE. SRE only."""
    uow = SqlUnitOfWork(get_session_factory())
    use_case = ActivateAssetUseCase(
        uow=uow, catalog=get_catalog_adapter(get_settings()), notifications=NoopNotificationAdapter()
    )
    from app.infrastructure.persistence.repositories.sql_asset_repository import SqlAssetRepository
    repo = SqlAssetRepository(session=session)
    from app.infrastructure.persistence.repositories.sql_endpoint_repository import SqlEndpointRepository
    endpoint_repo = SqlEndpointRepository(session=session)
    
    try:
        asset = await repo.find_by_name(asset_name)
        if not asset:
            raise AssetNotFoundError(f"Asset not found: {asset_name}")
            
        endpoint = await endpoint_repo.find_by_name(endpoint_name)
        if not endpoint:
            raise HTTPException(status_code=404, detail=f"Endpoint not found: {endpoint_name}")
            
        asset = await use_case.execute(asset.id, endpoint.id)
    except AssetNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except InvalidStateTransitionError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return asset_to_response(asset)

@router.put("/{asset_name}", response_model=AssetResponse)
async def update_asset(
    asset_name: str,
    body: AssetUpdateRequest,
    session: AsyncSession = Depends(get_db),
    _: CurrentUser = Depends(require_role(Role.PO_PM)),
) -> AssetResponse:
    """Update a DataAsset's fields. PO_PM only."""
    from app.application.assets.update_asset import UpdateAssetUseCase
    from app.infrastructure.persistence.repositories.sql_asset_repository import SqlAssetRepository
    from app.infrastructure.persistence.repositories.sql_endpoint_repository import SqlEndpointRepository
    
    uow = SqlUnitOfWork(get_session_factory())
    repo = SqlAssetRepository(session=session)
    endpoint_repo = SqlEndpointRepository(session=session)
    
    use_case = UpdateAssetUseCase(
        uow=uow, catalog=get_catalog_adapter(get_settings()), notifications=NoopNotificationAdapter()
    )
    
    asset = await repo.find_by_name(asset_name)
    if not asset:
        raise HTTPException(status_code=404, detail=f"Asset not found: {asset_name}")
        
    endpoint_id = None
    if body.endpoint_name:
        endpoint = await endpoint_repo.find_by_name(body.endpoint_name)
        if not endpoint:
            raise HTTPException(status_code=404, detail=f"Endpoint not found: {body.endpoint_name}")
        endpoint_id = endpoint.id
        
    try:
        updated = await use_case.execute(
            asset_id=asset.id,
            description=body.description,
            tags=body.tags,
            policy_tags=[t.value for t in body.policy_tags] if body.policy_tags is not None else None,
            endpoint_id=endpoint_id,
        )
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return asset_to_response(updated)

