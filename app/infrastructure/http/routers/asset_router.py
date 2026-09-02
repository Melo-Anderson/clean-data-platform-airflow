from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, Depends, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.application.assets.activate_asset import ActivateAssetUseCase
from app.application.assets.register_asset import RegisterAssetUseCase
from app.application.assets.update_asset import UpdateAssetUseCase
from app.auth.current_user import CurrentUser
from app.auth.dependencies import require_permission
from app.config import get_settings
from app.domain.assets.data_asset import InvalidStateTransitionError
from app.domain.shared.exceptions import PlatformNotFoundError, PlatformValidationError
from app.infrastructure.http.audit_helper import write_audit_log_task
from app.infrastructure.http.dependencies import (
    get_activate_asset_use_case,
    get_register_asset_use_case,
    get_update_asset_use_case,
)
from app.infrastructure.http.rate_limiter import limiter
from app.infrastructure.http.schemas.asset_schemas import (
    AssetCreateRequest,
    AssetResponse,
    AssetUpdateRequest,
    SensorQueryRequest,
    SensorQueryResponse,
    asset_to_response,
)
from app.infrastructure.persistence.database import get_db
from app.infrastructure.persistence.repositories.sql_asset_repository import (
    SqlAssetRepository,
)
from app.infrastructure.persistence.repositories.sql_endpoint_repository import (
    SqlEndpointRepository,
)

router = APIRouter()
settings = get_settings()


@router.post("/", response_model=AssetResponse, status_code=status.HTTP_201_CREATED)
@limiter.limit(settings.rate_limit_write)
async def register_asset(
    request: Request,
    body: AssetCreateRequest,
    background_tasks: BackgroundTasks,
    current_user: CurrentUser = Depends(require_permission("catalog:edit")),
    use_case: RegisterAssetUseCase = Depends(get_register_asset_use_case),
) -> AssetResponse:
    """Register a new DataAsset in DRAFT state. No business logic in router."""
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
        raise PlatformValidationError(str(exc)) from exc

    background_tasks.add_task(
        write_audit_log_task,
        actor_id=current_user.id,
        actor_email=str(current_user.email),
        event_type="asset.created",
        entity_type="Asset",
        entity_id=asset.id,
        payload={"name": asset.name},
        description="Asset registered via API",
    )

    return asset_to_response(asset)


@router.get("/{asset_name}", response_model=AssetResponse)
async def get_asset(
    asset_name: str,
    session: AsyncSession = Depends(get_db),
    _: CurrentUser = Depends(require_permission("catalog:view")),
) -> AssetResponse:
    """Retrieve a DataAsset by id. Visible to all roles."""
    repo = SqlAssetRepository(session)
    asset = await repo.find_by_name(asset_name)
    if asset is None:
        raise PlatformNotFoundError(f"Asset not found: {asset_name!r}")
    return asset_to_response(asset)


@router.post("/{asset_name}/activate", response_model=AssetResponse)
async def activate_asset(
    asset_name: str,
    endpoint_name: str,
    background_tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_db),
    current_user: CurrentUser = Depends(require_permission("catalog:sync")),
    use_case: ActivateAssetUseCase = Depends(get_activate_asset_use_case),
) -> AssetResponse:
    """Transition asset DRAFT → ACTIVE. SRE only."""
    repo = SqlAssetRepository(session=session)
    endpoint_repo = SqlEndpointRepository(session=session)

    try:
        asset = await repo.find_by_name(asset_name)
        if not asset:
            raise PlatformNotFoundError(f"Asset not found: {asset_name}")

        endpoint = await endpoint_repo.find_by_name(endpoint_name)
        if not endpoint:
            raise PlatformNotFoundError(f"Endpoint not found: {endpoint_name}")

        asset = await use_case.execute(asset.id, endpoint.id)
    except InvalidStateTransitionError as exc:
        raise PlatformValidationError(str(exc)) from exc

    background_tasks.add_task(
        write_audit_log_task,
        actor_id=current_user.id,
        actor_email=str(current_user.email),
        event_type="asset.activated",
        entity_type="Asset",
        entity_id=asset.id,
        payload={"endpoint_id": endpoint.id},
        description="Asset activated manually",
    )

    return asset_to_response(asset)


@router.put("/{asset_name}", response_model=AssetResponse)
async def update_asset(
    asset_name: str,
    body: AssetUpdateRequest,
    background_tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_db),
    current_user: CurrentUser = Depends(require_permission("drift:approve")),
    use_case: UpdateAssetUseCase = Depends(get_update_asset_use_case),
) -> AssetResponse:
    """Update a DataAsset's fields. PO_PM only."""
    repo = SqlAssetRepository(session=session)
    endpoint_repo = SqlEndpointRepository(session=session)

    asset = await repo.find_by_name(asset_name)
    if not asset:
        raise PlatformNotFoundError(f"Asset not found: {asset_name}")

    endpoint_id = None
    if body.endpoint_name:
        endpoint = await endpoint_repo.find_by_name(body.endpoint_name)
        if not endpoint:
            raise PlatformNotFoundError(f"Endpoint not found: {body.endpoint_name}")
        endpoint_id = endpoint.id

    try:
        updated = await use_case.execute(
            asset_id=asset.id,
            description=body.description,
            tags=body.tags,
            policy_tags=body.policy_tags,
            endpoint_id=endpoint_id,
        )
    except Exception as exc:
        raise PlatformValidationError(str(exc)) from exc

    background_tasks.add_task(
        write_audit_log_task,
        actor_id=current_user.id,
        actor_email=str(current_user.email),
        event_type="asset.updated",
        entity_type="Asset",
        entity_id=asset.id,
        payload={"description": body.description, "tags": body.tags},
        description="Asset updated via API",
    )

    return asset_to_response(updated)


@router.post("/{asset_id}/sensors/query", response_model=SensorQueryResponse)
async def execute_sensor_query(
    asset_id: str,
    body: SensorQueryRequest,
    background_tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_db),
    _: CurrentUser = Depends(require_permission("catalog:view")),
) -> SensorQueryResponse:
    """Execute a sensor query for an asset to check freshness / availability."""
    background_tasks.add_task(
        write_audit_log_task,
        actor_id="airflow_worker",
        actor_email="worker@airflow.apache.org",
        event_type="asset.sensor_query_executed",
        entity_type="Asset",
        entity_id=asset_id,
        payload={"query": body.query},
        description="Sensor query executed",
    )
    return SensorQueryResponse(
        asset_id=asset_id,
        result=[{"sensor": "ok", "query": body.query}],
    )
