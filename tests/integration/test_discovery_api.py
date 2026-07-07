from __future__ import annotations

import pytest
from httpx import AsyncClient

from app.domain.assets.asset_state import AssetState
from app.infrastructure.persistence.models.data_asset_model import DataAssetModel
from app.infrastructure.persistence.models.data_object_model import DataObjectModel
from app.infrastructure.persistence.models.discovery_run_model import DiscoveryRunModel
from app.infrastructure.persistence.models.drift_approval_model import DriftApprovalModel
from app.infrastructure.persistence.models.endpoint_model import EndpointModel


@pytest.mark.asyncio
async def test_trigger_discovery_run_success(po_pm_client: AsyncClient, db_session) -> None:
    # 1. Setup Data
    endpoint = EndpointModel(
        id="ep-1",
        name="db-prod",
        type="database",
        credential_ref="secret",
        technical_description="",
        subtype_data={}
    )
    db_session.add(endpoint)

    asset = DataAssetModel(
        id="asset-1",
        name="test-asset",
        description="desc",
        owner_email="owner@co.com",
        state=AssetState.ACTIVE.value,
        discovery_schedule="0 * * * *",
        discovery_scope={"include": [], "exclude": []},
        tags=[],
        policy_tags=[],
        endpoint_id="ep-1"
    )
    db_session.add(asset)
    await db_session.commit()

    # 2. Execute
    response = await po_pm_client.post(
        "/discovery/assets/test-asset/run",
        json={"triggered_by": "manual_test"}
    )

    # 3. Verify
    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "completed"
    assert data["asset_id"] == "asset-1"
    assert data["triggered_by"] == "manual_test"


@pytest.mark.asyncio
async def test_decide_drift_approval_success(po_pm_client: AsyncClient, db_session) -> None:
    # 1. Setup Data
    run = DiscoveryRunModel(id="run-1", asset_id="asset-1", triggered_by="system", status="completed")
    db_session.add(run)

    approval = DriftApprovalModel(
        id="app-1",
        discovery_run_id="run-1",
        asset_id="asset-1",
        object_id="obj-1",
        field_name="col1",
        change_type="type_incompatible",
        severity_description="critical drift",
        decision="pending"
    )
    
    obj = DataObjectModel(
        id="obj-1",
        asset_id="asset-1",
        name="users",
        type="TABLE"
    )
    db_session.add(run)
    db_session.add(approval)
    db_session.add(obj)
    await db_session.commit()

    # 2. Execute
    response = await po_pm_client.post(
        "/discovery/approvals/app-1/decision",
        json={"decision": "approved", "decided_by": "owner@co.com", "notes": "ok"}
    )

    # 3. Verify
    assert response.status_code == 200
    data = response.json()
    assert data["decision"] == "approved"
    assert data["decided_by"] == "owner@co.com"


@pytest.mark.asyncio
async def test_trigger_discovery_run_missing_endpoint(po_pm_client: AsyncClient, db_session) -> None:
    # 1. Setup Data: Asset with NO endpoint
    asset = DataAssetModel(
        id="asset-no-ep",
        name="test-asset-no-ep",
        description="desc",
        owner_email="owner@co.com",
        state=AssetState.ACTIVE.value,
        discovery_schedule="0 * * * *",
        discovery_scope={"include": [], "exclude": []},
        tags=[],
        policy_tags=[],
        endpoint_id=None
    )
    db_session.add(asset)
    await db_session.commit()

    # 2. Execute
    response = await po_pm_client.post(
        "/discovery/assets/test-asset-no-ep/run",
        json={"triggered_by": "manual_test"}
    )

    # 3. Verify
    assert response.status_code == 422
    assert "Asset has no endpoint" in response.json()["detail"]


@pytest.mark.asyncio
async def test_decide_drift_approval_invalid_decision(po_pm_client: AsyncClient, db_session) -> None:
    # 1. Setup Data
    run = DiscoveryRunModel(id="run-invalid", asset_id="asset-1", triggered_by="system", status="completed")
    approval = DriftApprovalModel(
        id="app-invalid",
        discovery_run_id="run-invalid",
        asset_id="asset-1",
        object_id="obj-1",
        field_name="col1",
        change_type="type_incompatible",
        severity_description="critical drift",
        decision="pending"
    )
    db_session.add(run)
    db_session.add(approval)
    await db_session.commit()

    # 2. Execute
    response = await po_pm_client.post(
        "/discovery/approvals/app-invalid/decision",
        json={"decision": "maybe", "decided_by": "owner@co.com"}
    )

    # 3. Verify
    assert response.status_code == 422
    assert "Decision must be 'approved', 'rejected' or 'pending'" in response.json()["detail"]
