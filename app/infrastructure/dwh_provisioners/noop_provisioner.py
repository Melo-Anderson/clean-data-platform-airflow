from __future__ import annotations

from typing import Any


class NoOpDwhProvisioner:
    async def ensure_dataset_exists(
        self, dataset_id: str, description: str = "", labels: dict[str, str] | None = None
    ) -> None:
        pass

    async def ensure_table_exists(
        self,
        dataset_id: str,
        table_id: str,
        description: str = "",
        labels: dict[str, str] | None = None,
        schema_fields: list[dict[str, Any]] | None = None,
    ) -> None:
        pass
