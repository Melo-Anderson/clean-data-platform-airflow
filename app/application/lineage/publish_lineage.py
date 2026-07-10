from __future__ import annotations

from app.application.shared.adapters.catalog_adapter import CatalogAdapter
from app.application.unit_of_work import UnitOfWork


class LineageMappingNotFoundError(Exception):
    pass


class PublishLineageToCatalogUseCase:
    """
    Use case triggered after lineage consolidation in pipelines
    to push column lineage edges to the corporate catalog.
    """

    def __init__(
        self,
        uow: UnitOfWork,
        catalog_adapter: CatalogAdapter,
    ) -> None:
        self._uow = uow
        self._catalog = catalog_adapter

    async def execute(self, *, lineage_mapping_id: str) -> None:
        async with self._uow as uow:
            mapping = await uow.lineage.find_by_id(lineage_mapping_id)
            if not mapping:
                raise LineageMappingNotFoundError(
                    f"Lineage mapping not found for ID: {lineage_mapping_id}. Expected a valid UUID."
                )

        # Catalog publication occurs asynchronously to prevent metadata catalog
        # failures from breaking data pipeline execution.
        await self._catalog.publish_lineage(mapping)
