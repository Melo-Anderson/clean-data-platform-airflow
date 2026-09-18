from __future__ import annotations

import pytest

from app.domain.assets.asset_state import AssetState
from app.domain.assets.data_asset import DataAsset
from app.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress
from app.infrastructure.adapters.dataform.dataform_catalog_adapter import DataformCatalogAdapter
from app.infrastructure.adapters.dataform.dataform_compilation_parser import (
    DataformColumnMetadata,
    DataformParsedMetadata,
    DataformTableMetadata,
)
from app.infrastructure.persistence.database import get_session_factory
from app.infrastructure.persistence.sql_unit_of_work import SqlUnitOfWork


@pytest.mark.asyncio
async def test_dataform_catalog_adapter_syncs_tables_and_elements() -> None:
    uow = SqlUnitOfWork(get_session_factory())

    async with uow:
        asset = DataAsset(
            id="asset-dataform-test",
            name="platform_dataform_asset",
            description="Dataform Transformation Asset",
            owner=EmailAddress("analytics@co.com"),
            state=AssetState.ACTIVE,
            endpoint_id=None,
            discovery_schedule=CronSchedule("0 4 * * *"),
            discovery_scope=DiscoveryScope(include=[]),
        )
        await uow.assets.save(asset)
        await uow.commit()

    metadata = DataformParsedMetadata(
        tables=[
            DataformTableMetadata(
                name="dim_players",
                schema="platform_gold",
                type="table",
                description="Players Dimension",
                dependency_targets=["slv_players"],
                columns=[
                    DataformColumnMetadata(
                        name="player_sk", data_type="STRING", description="Surrogate key"
                    ),
                    DataformColumnMetadata(
                        name="score", data_type="INT64", description="Total score"
                    ),
                ],
            )
        ],
        declarations=[],
        assertions=[],
        lineage={"dim_players": ["slv_players"]},
    )

    adapter = DataformCatalogAdapter(uow=uow)
    sync_result = await adapter.sync_metadata(asset_id="asset-dataform-test", metadata=metadata)

    assert sync_result.objects_synced == 1
    assert sync_result.elements_synced == 2

    async with uow:
        saved_objs = await uow.objects.find_by_asset_id("asset-dataform-test")
        assert len(saved_objs) == 1
        assert saved_objs[0].name == "dim_players"
        assert len(saved_objs[0].elements) == 2
