import pytest

from app.domain.assets.asset_state import AssetState
from app.domain.assets.data_asset import DataAsset
from app.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress
from app.infrastructure.adapters.dbt.dbt_catalog_adapter import DbtCatalogAdapter
from app.infrastructure.adapters.dbt.dbt_manifest_parser import (
    DbtColumnMetadata,
    DbtNodeMetadata,
    DbtParsedManifest,
)
from app.infrastructure.persistence.database import get_session_factory
from app.infrastructure.persistence.sql_unit_of_work import SqlUnitOfWork


@pytest.mark.asyncio
async def test_dbt_catalog_adapter_syncs_models_and_elements_to_database() -> None:
    uow = SqlUnitOfWork(get_session_factory())

    async with uow:
        asset = DataAsset(
            id="asset-dbt-test",
            name="platform_transformation_asset",
            description="dbt Transformation Asset",
            owner=EmailAddress("analytics@co.com"),
            state=AssetState.ACTIVE,
            endpoint_id=None,
            discovery_schedule=CronSchedule("0 4 * * *"),
            discovery_scope=DiscoveryScope(include=[]),
        )
        await uow.assets.save(asset)
        await uow.commit()

    manifest = DbtParsedManifest(
        models=[
            DbtNodeMetadata(
                unique_id="model.platform.dim_players",
                name="dim_players",
                resource_type="model",
                schema="platform_gold",
                description="Players Dimension",
                depends_on_nodes=["model.platform.slv_players"],
                columns=[
                    DbtColumnMetadata(
                        name="player_sk", data_type="STRING", description="Surrogate key"
                    ),
                    DbtColumnMetadata(name="email", data_type="STRING", description="Email"),
                ],
            )
        ],
        sources=[],
        lineage={"model.platform.dim_players": ["model.platform.slv_players"]},
    )

    adapter = DbtCatalogAdapter(uow=uow)
    sync_result = await adapter.sync_manifest(asset_id="asset-dbt-test", manifest=manifest)

    assert sync_result.objects_synced == 1
    assert sync_result.elements_synced == 2

    async with uow:
        saved_objs = await uow.objects.find_by_asset_id("asset-dbt-test")
        assert len(saved_objs) == 1
        assert saved_objs[0].name == "dim_players"
