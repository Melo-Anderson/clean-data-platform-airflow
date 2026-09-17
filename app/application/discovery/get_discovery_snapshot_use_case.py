# app/application/discovery/get_discovery_snapshot_use_case.py
from __future__ import annotations

from typing import Any

from app.application.unit_of_work import UnitOfWork
from app.domain.shared.exceptions import PlatformNotFoundError


class GetDiscoverySnapshotUseCase:
    """Retrieves discovered schema snapshot and elements for a data asset."""

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def execute(self, asset_name: str) -> dict[str, Any]:
        async with self._uow as uow:
            asset = await uow.assets.find_by_name(asset_name)
            if not asset:
                raise PlatformNotFoundError(f"Asset not found: {asset_name}")

            objects = await uow.objects.find_by_asset_id(asset.id)
            objects_dict = {
                obj.name: {
                    "fields": [
                        {
                            "name": elem.name,
                            "normalized_type": elem.destination_type.value
                            if elem.destination_type
                            else "string",
                            "source_type": elem.source_type.value if elem.source_type else None,
                            "nullable": elem.nullable,
                            "is_primary_key": elem.is_primary_key,
                        }
                        for elem in obj.elements
                    ]
                }
                for obj in objects
            }
            all_fields = [f for obj_data in objects_dict.values() for f in obj_data["fields"]]
            return {
                "asset_id": asset.id,
                "asset_name": asset.name,
                "objects": objects_dict,
                "fields": all_fields,
            }
