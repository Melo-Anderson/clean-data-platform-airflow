from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from app.application.assets.register_asset import RegisterAssetUseCase
from app.application.unit_of_work import UnitOfWork
from app.domain.assets.asset_state import AssetState
from app.domain.assets.data_asset import DataAsset
from app.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress
from app.infrastructure.adapters.catalog.noop_adapter import NoopCatalogAdapter
from app.infrastructure.adapters.notifications.noop_notification_adapter import (
    NoopNotificationAdapter,
)


class MockUoW(UnitOfWork):
    def __init__(self) -> None:
        self.commit_called = False
        self.rollback_called = False
        self.assets = AsyncMock()
        self.endpoints = AsyncMock()
        from unittest.mock import MagicMock

        self._audit_logs = MagicMock()

    @property
    def assets(self):
        return self._assets

    @assets.setter
    def assets(self, value) -> None:
        self._assets = value

    @property
    def endpoints(self):
        return self._endpoints

    @endpoints.setter
    def endpoints(self, value) -> None:
        self._endpoints = value

    @property
    def audit_logs(self):
        return self._audit_logs

    @audit_logs.setter
    def audit_logs(self, value) -> None:
        self._audit_logs = value

    async def commit(self) -> None:
        self.commit_called = True

    async def rollback(self) -> None:
        self.rollback_called = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            await self.rollback()


@pytest.mark.asyncio
async def test_register_asset_calls_adapters_after_commit() -> None:
    uow = MockUoW()
    catalog = AsyncMock(spec=NoopCatalogAdapter)
    notifications = AsyncMock(spec=NoopNotificationAdapter)

    # Mocking the repository return
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
    uow.assets.save.return_value = dummy_asset

    use_case = RegisterAssetUseCase(uow=uow, catalog=catalog, notifications=notifications)

    asset = await use_case.execute(
        name="test",
        description="desc",
        owner_email="t@co.com",
        tags=[],
        policy_tags=[],
        discovery_schedule="0 * * * *",
        discovery_scope_include=["*"],
        discovery_scope_exclude=[],
    )

    assert uow.commit_called is True
    catalog.publish_asset.assert_called_once_with(
        asset_id=asset.id, name=asset.name, state=asset.state.value, metadata={}
    )
    notifications.send_alert.assert_called_once()
