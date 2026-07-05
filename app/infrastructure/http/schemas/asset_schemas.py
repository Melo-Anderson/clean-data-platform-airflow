from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from app.domain.assets.asset_state import AssetState
from app.domain.assets.data_asset import DataAsset
from app.domain.shared.policy_tag import PolicyTag


class AssetCreateRequest(BaseModel):
    name: str
    description: str
    owner_email: str
    tags: list[str] = []
    policy_tags: list[PolicyTag] = []
    discovery_schedule: str
    discovery_scope_include: list[str] = []
    discovery_scope_exclude: list[str] = []


class AssetScopeUpdateRequest(BaseModel):
    discovery_scope_include: list[str]
    discovery_scope_exclude: list[str]


class AssetUpdateRequest(BaseModel):
    description: str | None = None
    tags: list[str] | None = None
    policy_tags: list[PolicyTag] | None = None
    # Usuário informa nome do endpoint; o Router resolve para ID antes de chamar o Use Case
    endpoint_name: str | None = None


class AssetResponse(BaseModel):
    model_config = ConfigDict(from_attributes=False)

    id: str
    name: str
    description: str
    owner_email: str
    tags: list[str]
    policy_tags: list[PolicyTag]
    state: AssetState
    discovery_schedule: str
    discovery_scope_include: list[str]
    discovery_scope_exclude: list[str]
    endpoint_id: str | None


def asset_to_response(asset: DataAsset) -> AssetResponse:
    """Map domain entity to HTTP response. Transport layer only."""
    return AssetResponse(
        id=asset.id,
        name=asset.name,
        description=asset.description,
        owner_email=asset.owner.value,
        tags=asset.tags,
        policy_tags=asset.policy_tags,
        state=asset.state,
        discovery_schedule=asset.discovery_schedule.expression,
        discovery_scope_include=list(asset.discovery_scope.include),
        discovery_scope_exclude=list(asset.discovery_scope.exclude),
        endpoint_id=asset.endpoint_id,
    )
