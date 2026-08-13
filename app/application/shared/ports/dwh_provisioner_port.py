from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class DwhProvisionerPort(Protocol):
    """Port for provisioning Data Warehouse datasets and tables."""

    async def ensure_dataset_exists(
        self, dataset_id: str, description: str = "", labels: dict[str, str] | None = None
    ) -> None: ...

    async def ensure_table_exists(
        self,
        dataset_id: str,
        table_id: str,
        description: str = "",
        labels: dict[str, str] | None = None,
        schema_fields: list[dict[str, Any]] | None = None,
    ) -> None: ...
