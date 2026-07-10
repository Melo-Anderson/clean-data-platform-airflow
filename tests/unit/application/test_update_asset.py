from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from app.application.assets.update_asset import UpdateAssetUseCase
from app.domain.assets.asset_state import AssetState
from app.domain.assets.data_asset import DataAsset
from app.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress
from app.infrastructure.adapters.catalog.noop_adapter import NoopCatalogAdapter
from app.infrastructure.adapters.notifications.noop_notification_adapter import (
    NoopNotificationAdapter,
)
from tests.unit.application.test_register_asset import MockUoW


@pytest.mark.asyncio
async def test_update_asset_success() -> None:
    uow = MockUoW()
    catalog = AsyncMock(spec=NoopCatalogAdapter)
    notifications = AsyncMock(spec=NoopNotificationAdapter)

    asset = DataAsset(
        id="a1",
        name="test-asset",
        description="old desc",
        owner=EmailAddress("t@co.com"),
        tags=["core"],
        policy_tags=[],
        state=AssetState.ACTIVE,
        discovery_schedule=CronSchedule("0 * * * *"),
        discovery_scope=DiscoveryScope(),
    )
    uow.assets.update.return_value = asset

    use_case = UpdateAssetUseCase(uow=uow, catalog=catalog, notifications=notifications)
    await use_case.execute(
        asset_id="a1",
        description="new desc",
        tags=["reporting"],
        endpoint_id="ep1",
    )

    assert uow.commit_called is True
    catalog.publish_asset.assert_called_once()
    notifications.send_alert.assert_called_once()
