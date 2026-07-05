from __future__ import annotations

from typing import Protocol, runtime_checkable

from app.domain.assets.asset_state import AssetState
from app.domain.assets.data_asset import DataAsset
from app.domain.shared.value_objects import DiscoveryScope


@runtime_checkable
class AssetRepository(Protocol):
    """
    Repository interface for DataAsset persistence.

    Defined in domain — concrete implementations live in infrastructure.
    Domain services depend only on this Protocol.
    """

    async def save(self, asset: DataAsset) -> DataAsset: ...

    async def find_by_id(self, asset_id: str) -> DataAsset | None: ...

    async def find_by_name(self, name: str) -> DataAsset | None: ...

    async def update(self, asset: DataAsset) -> DataAsset: ...

    async def update_state(self, asset_id: str, new_state: AssetState) -> DataAsset: ...

    async def update_endpoint(self, asset_id: str, endpoint_id: str) -> DataAsset: ...

    async def update_scope(self, asset_id: str, scope: DiscoveryScope) -> DataAsset: ...
