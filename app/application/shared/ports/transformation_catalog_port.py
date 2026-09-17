from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class TransformationCatalogSyncResult:
    """Result of a catalog synchronization for any transformation engine."""

    synced: bool
    objects_synced: int = 0
    elements_synced: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "synced": self.synced,
            "objects_synced": self.objects_synced,
            "elements_synced": self.elements_synced,
        }


@runtime_checkable
class TransformationCatalogAdapter(Protocol):
    """Port for synchronizing transformation models and columns into the platform catalog."""

    async def sync_catalog(
        self, asset_id: str, manifest_path: str | Path
    ) -> TransformationCatalogSyncResult: ...
