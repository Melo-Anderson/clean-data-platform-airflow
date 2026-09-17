# tests/unit/application/test_run_discovery_use_case_by_name.py
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.application.discovery.run_discovery_use_case import RunDiscoveryUseCase
from app.domain.assets.data_asset import DataAsset
from app.domain.shared.exceptions import PlatformNotFoundError, PlatformValidationError
from app.domain.shared.value_objects import EmailAddress
from tests.unit.fakes import FakeUnitOfWork


@pytest.mark.asyncio
async def test_run_discovery_raises_not_found_when_asset_name_missing() -> None:
    """RunDiscoveryUseCase deve levantar PlatformNotFoundError se asset_name nao existir."""
    uow = FakeUnitOfWork()
    use_case = RunDiscoveryUseCase(
        uow=uow,
        runner_factory=MagicMock(),
        drift_service=MagicMock(),
        self_healing=MagicMock(),
        provisioning_service=MagicMock(),
    )
    with pytest.raises(PlatformNotFoundError, match="Asset not found"):
        await use_case.execute(asset_name="nonexistent-asset", triggered_by="test")


@pytest.mark.asyncio
async def test_run_discovery_raises_validation_error_when_asset_by_name_has_no_endpoint() -> None:
    """RunDiscoveryUseCase deve levantar PlatformValidationError se o asset buscado por nome nao possuir endpoint."""
    uow = FakeUnitOfWork()
    asset = DataAsset(
        id="asset-no-ep",
        name="asset-without-endpoint",
        description="desc",
        owner=EmailAddress("owner@co.com"),
        endpoint_id=None,
    )
    await uow.assets.save(asset)

    use_case = RunDiscoveryUseCase(
        uow=uow,
        runner_factory=MagicMock(),
        drift_service=MagicMock(),
        self_healing=MagicMock(),
        provisioning_service=MagicMock(),
    )
    with pytest.raises(PlatformValidationError, match="Asset has no endpoint: asset-no-ep"):
        await use_case.execute(asset_name="asset-without-endpoint", triggered_by="test")
