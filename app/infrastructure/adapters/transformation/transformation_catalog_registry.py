from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path

from app.application.shared.ports.transformation_catalog_port import (
    TransformationCatalogAdapter,
    TransformationCatalogSyncResult,
)
from app.application.unit_of_work import UnitOfWork
from app.infrastructure.adapters.dbt.dbt_catalog_adapter import DbtCatalogAdapter
from app.infrastructure.adapters.dbt.dbt_manifest_parser import DbtManifestParser
from app.infrastructure.persistence.database import get_session_factory
from app.infrastructure.persistence.sql_unit_of_work import SqlUnitOfWork

logger = logging.getLogger(__name__)


class DbtCatalogAdapterWrapper:
    """Wraps DbtCatalogAdapter to implement TransformationCatalogAdapter.

    Receives a UnitOfWork factory by injection (§6.2) to avoid
    coupling the wrapper to SqlUnitOfWork/get_session_factory directly.
    """

    def __init__(self, uow_factory: Callable[[], UnitOfWork]) -> None:
        self._uow_factory = uow_factory
        self._parser = DbtManifestParser()

    async def sync_catalog(
        self, asset_id: str, manifest_path: str | Path
    ) -> TransformationCatalogSyncResult:
        path = Path(manifest_path)
        if not path.exists():
            raise FileNotFoundError(f"Manifest file not found at {manifest_path}")

        manifest = self._parser.parse_file(path)
        uow = self._uow_factory()
        adapter = DbtCatalogAdapter(uow=uow)
        result = await adapter.sync_manifest(asset_id=asset_id, manifest=manifest)
        return TransformationCatalogSyncResult(
            synced=True,
            objects_synced=result.objects_synced,
            elements_synced=result.elements_synced,
        )


class TransformationCatalogRegistry:
    """Registry for TransformationCatalogAdapter implementations.

    Follows the same fail-fast pattern as ComputeAdapterRegistry:
    .get() raises ValueError for unregistered engines (never returns None).
    """

    _factories: dict[str, Callable[[], TransformationCatalogAdapter]] = {}

    @classmethod
    def register(cls, engine: str, factory: Callable[[], TransformationCatalogAdapter]) -> None:
        cls._factories[engine.lower()] = factory

    @classmethod
    def get(cls, engine: str) -> TransformationCatalogAdapter:
        factory = cls._factories.get(engine.lower())
        if factory is None:
            raise ValueError(
                f"No catalog adapter registered for transformation engine '{engine}'. "
                f"Registered engines: {cls.list_engines()}"
            )
        return factory()

    @classmethod
    def list_engines(cls) -> list[str]:
        return sorted(cls._factories.keys())


def _dbt_catalog_factory() -> DbtCatalogAdapterWrapper:
    return DbtCatalogAdapterWrapper(uow_factory=lambda: SqlUnitOfWork(get_session_factory()))


TransformationCatalogRegistry.register("dbt", _dbt_catalog_factory)
