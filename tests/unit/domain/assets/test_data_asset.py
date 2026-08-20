from __future__ import annotations

import pytest

from app.domain.assets.asset_state import AssetState
from app.domain.assets.data_asset import DataAsset, InvalidStateTransitionError
from app.domain.shared.value_objects import DiscoveryScope, EmailAddress


def make_test_asset(state: AssetState = AssetState.DRAFT) -> DataAsset:
    return DataAsset(
        id="asset-001",
        name="customers_domain",
        description="Customer master data",
        owner=EmailAddress("data-owner@company.com"),
        state=state,
    )


def test_data_asset_activate_success() -> None:
    asset = make_test_asset(AssetState.DRAFT)
    asset.activate(endpoint_id="ep-pg-01")
    assert asset.state == AssetState.ACTIVE
    assert asset.endpoint_id == "ep-pg-01"


def test_data_asset_activate_requires_endpoint_id() -> None:
    asset = make_test_asset(AssetState.DRAFT)
    with pytest.raises(ValueError, match="endpoint_id cannot be empty"):
        asset.activate(endpoint_id="")


def test_data_asset_invalid_transition_raises() -> None:
    asset = make_test_asset(AssetState.DRAFT)
    with pytest.raises(InvalidStateTransitionError):
        asset.deprecate()


def test_data_asset_deprecate_and_archive() -> None:
    asset = make_test_asset(AssetState.DRAFT)
    asset.activate(endpoint_id="ep-pg-01")
    asset.deprecate()
    assert asset.state == AssetState.DEPRECATED
    asset.archive()
    assert asset.state == AssetState.ARCHIVED


def test_data_asset_update_scope() -> None:
    asset = make_test_asset(AssetState.ACTIVE)
    new_scope = DiscoveryScope(include=["orders", "payments"])
    asset.update_scope(new_scope)
    assert asset.discovery_scope == new_scope
