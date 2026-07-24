from __future__ import annotations

from typing import Protocol, runtime_checkable

from app.domain.discovery.discovery_run import DiscoveryRun


@runtime_checkable
class DiscoveryRunRepository(Protocol):
    async def save(self, run: DiscoveryRun) -> DiscoveryRun:
        ...

    async def find_by_id(self, run_id: str) -> DiscoveryRun | None:
        ...

    async def find_latest_by_asset_id(self, asset_id: str) -> DiscoveryRun | None:
        """Most recent terminal run for dashboard/diff baseline."""
        ...

    async def find_all_by_asset_id(self, asset_id: str, *, limit: int = 20) -> list[DiscoveryRun]:
        ...
