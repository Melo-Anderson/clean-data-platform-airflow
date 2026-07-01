from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from app.application.assets.activate_asset import ActivateAssetUseCase
from app.domain.assets.asset_state import AssetState
from app.domain.assets.data_asset import DataAsset
from app.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress
from app.infrastructure.adapters.catalog.noop_catalog_adapter import NoopCatalogAdapter
from app.infrastructure.adapters.notifications.noop_notification_adapter import (
    NoopNotificationAdapter,
)
from tests.unit.application.test_register_asset import MockUoW


@pytest.mark.asyncio
async def test_activate_asset_calls_adapters_after_commit():
    uow = MockUoW()
    catalog = AsyncMock(spec=NoopCatalogAdapter)
    notifications = AsyncMock(spec=NoopNotificationAdapter)

    dummy_asset = DataAsset(
        id="a1",
        name="test",
        description="desc",
        owner=EmailAddress("t@co.com"),
        tags=[],
        policy_tags=[],
        state=AssetState.DRAFT,
        discovery_schedule=CronSchedule("0 * * * *"),
        discovery_scope=DiscoveryScope(),
    )
    active_asset = DataAsset(
        id="a1",
        name="test",
        description="desc",
        owner=EmailAddress("t@co.com"),
        tags=[],
        policy_tags=[],
        state=AssetState.ACTIVE,
        discovery_schedule=CronSchedule("0 * * * *"),
        discovery_scope=DiscoveryScope(),
        endpoint_id="ep1",
    )
    uow.assets.find_by_id.return_value = dummy_asset
    uow.assets.update_endpoint.return_value = None
    uow.assets.update_state.return_value = active_asset

    use_case = ActivateAssetUseCase(uow=uow, catalog=catalog, notifications=notifications)

    asset = await use_case.execute("a1", "ep1")

    assert uow.commit_called is True
    catalog.publish_asset.assert_called_once_with(
        asset_id=asset.id, name=asset.name, state="active", metadata={"endpoint_id": "ep1"}
    )
    notifications.send_alert.assert_called_once()
