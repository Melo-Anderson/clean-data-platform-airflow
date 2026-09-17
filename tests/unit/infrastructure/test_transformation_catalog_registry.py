from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from app.application.shared.ports.transformation_catalog_port import (
    TransformationCatalogAdapter,
    TransformationCatalogSyncResult,
)
from app.infrastructure.adapters.transformation.transformation_catalog_registry import (
    DbtCatalogAdapterWrapper,
    TransformationCatalogRegistry,
)


class StubCatalogAdapter:
    async def sync_catalog(
        self, asset_id: str, manifest_path: str | Path
    ) -> TransformationCatalogSyncResult:
        return TransformationCatalogSyncResult(synced=True, objects_synced=5, elements_synced=20)


def test_registry_registers_and_retrieves_adapter() -> None:
    TransformationCatalogRegistry.register("stub", lambda: StubCatalogAdapter())
    adapter = TransformationCatalogRegistry.get("stub")
    assert isinstance(adapter, TransformationCatalogAdapter)


def test_registry_raises_for_unregistered_engine() -> None:
    with pytest.raises(ValueError) as exc_info:
        TransformationCatalogRegistry.get("unregistered_engine_xyz")
    assert "unregistered_engine_xyz" in str(exc_info.value)


def test_registry_has_dbt_registered_by_default() -> None:
    adapter = TransformationCatalogRegistry.get("dbt")
    assert adapter is not None


@pytest.mark.asyncio
async def test_dbt_catalog_adapter_wrapper_raises_filenotfound_when_manifest_missing(
    tmp_path: Path,
) -> None:
    stub_uow = MagicMock()
    wrapper = DbtCatalogAdapterWrapper(uow_factory=lambda: stub_uow)
    missing = tmp_path / "missing_manifest.json"
    with pytest.raises(FileNotFoundError) as exc_info:
        await wrapper.sync_catalog(asset_id="asset-1", manifest_path=missing)
    assert "Manifest file not found" in str(exc_info.value)


def test_sync_result_to_dict_contains_all_fields() -> None:
    result = TransformationCatalogSyncResult(synced=True, objects_synced=3, elements_synced=10)
    d = result.to_dict()
    assert d == {"synced": True, "objects_synced": 3, "elements_synced": 10}
