from __future__ import annotations

from app.domain.assets.asset_repository import AssetRepository
from app.domain.assets.asset_state import VALID_TRANSITIONS, AssetState
from app.domain.assets.data_asset import DataAsset
from app.domain.shared.policy_tag import PolicyTag
from app.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress


class AssetNotFoundError(Exception):
    def __init__(self, asset_id: str) -> None:
        super().__init__(f"DataAsset not found: id={asset_id!r}")
        self.asset_id = asset_id


class InvalidStateTransitionError(Exception):
    def __init__(self, current: AssetState, target: AssetState) -> None:
        allowed = sorted(VALID_TRANSITIONS[current])
        super().__init__(
            f"Cannot transition from '{current}' to '{target}'. "
            f"Allowed targets from '{current}': {allowed}"
        )


class AssetService:
    """
    Domain service for DataAsset lifecycle management.

    No FastAPI. No SQLAlchemy. Depends only on AssetRepository Protocol.

    Example:
        service = AssetService(repo=FakeAssetRepository())
        asset = await service.register(name="customers", owner=EmailAddress("po@co.com"), ...)
    """

    def __init__(self, repo: AssetRepository) -> None:
        self._repo = repo

    async def register(
        self,
        asset_id: str,
        name: str,
        description: str,
        owner: EmailAddress,
        tags: list[str],
        policy_tags: list[PolicyTag],
        discovery_schedule: CronSchedule,
        discovery_scope: DiscoveryScope,
    ) -> DataAsset:
        """Create a new DataAsset in DRAFT state and persist it."""
        asset = DataAsset(
            id=asset_id,
            name=name,
            description=description,
            owner=owner,
            tags=tags,
            policy_tags=policy_tags,
            state=AssetState.DRAFT,
            discovery_schedule=discovery_schedule,
            discovery_scope=discovery_scope,
        )
        return await self._repo.save(asset)

    async def transition_to_active(self, asset_id: str, endpoint_id: str) -> DataAsset:
        """Move asset DRAFT → ACTIVE after SRE provisions the Endpoint."""
        asset = await self._require_asset(asset_id)
        self._assert_transition(asset.state, AssetState.ACTIVE)
        await self._repo.update_endpoint(asset_id, endpoint_id)
        return await self._repo.update_state(asset_id, AssetState.ACTIVE)

    async def deprecate(self, asset_id: str) -> DataAsset:
        """Move asset ACTIVE → DEPRECATED."""
        asset = await self._require_asset(asset_id)
        self._assert_transition(asset.state, AssetState.DEPRECATED)
        return await self._repo.update_state(asset_id, AssetState.DEPRECATED)

    async def archive(self, asset_id: str) -> DataAsset:
        """Move asset DEPRECATED → ARCHIVED."""
        asset = await self._require_asset(asset_id)
        self._assert_transition(asset.state, AssetState.ARCHIVED)
        return await self._repo.update_state(asset_id, AssetState.ARCHIVED)

    async def update_scope(self, asset_id: str, scope: DiscoveryScope) -> DataAsset:
        """Update discovery_scope. No SRE involvement required."""
        await self._require_asset(asset_id)
        return await self._repo.update_scope(asset_id, scope)

    async def update(
        self,
        asset_id: str,
        description: str | None = None,
        owner: EmailAddress | None = None,
        tags: list[str] | None = None,
        policy_tags: list[PolicyTag] | None = None,
        discovery_schedule: CronSchedule | None = None,
        discovery_scope: DiscoveryScope | None = None,
        endpoint_id: str | None = None,
    ) -> DataAsset:
        """Update multiple DataAsset fields at once."""
        asset = await self._require_asset(asset_id)
        if description is not None:
            asset.description = description
        if owner is not None:
            asset.owner = owner
        if tags is not None:
            asset.tags = tags
        if policy_tags is not None:
            asset.policy_tags = policy_tags
        if discovery_schedule is not None:
            asset.discovery_schedule = discovery_schedule
        if discovery_scope is not None:
            asset.discovery_scope = discovery_scope
        if endpoint_id is not None:
            asset.endpoint_id = endpoint_id

        return await self._repo.update(asset)

    async def _require_asset(self, asset_id: str) -> DataAsset:
        asset = await self._repo.find_by_id(asset_id)
        if asset is None:
            raise AssetNotFoundError(asset_id)
        return asset

    @staticmethod
    def _assert_transition(current: AssetState, target: AssetState) -> None:
        if target not in VALID_TRANSITIONS[current]:
            raise InvalidStateTransitionError(current, target)
