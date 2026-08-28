from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.application.discovery.run_discovery_use_case import RunDiscoveryUseCase
from app.domain.assets.asset_state import AssetState
from app.domain.assets.data_asset import DataAsset
from app.domain.discovery.discovery_run_status import DiscoveryRunStatus
from app.domain.endpoints.endpoint import FileSystemEndpoint
from app.domain.shared.value_objects import (
    CredentialReference,
    CronSchedule,
    DiscoveryScope,
    EmailAddress,
)
from app.infrastructure.adapters.secrets.noop_secret_manager_adapter import NoopSecretManagerAdapter
from app.infrastructure.discovery.discovery_runner_factory import DiscoveryRunnerFactoryImpl
from app.infrastructure.persistence.database import get_session_factory
from app.infrastructure.persistence.sql_unit_of_work import SqlUnitOfWork


@pytest.mark.asyncio
async def test_run_filesystem_discovery_full_cycle(tmp_path: Path) -> None:

    # Setup test CSV file
    csv_file = tmp_path / "vendas_2026.csv"
    csv_file.write_text("id,total,status\n1,100.50,PAID\n2,20.00,REFUNDED\n", encoding="utf-8")

    # Setup test JSON file
    json_file = tmp_path / "itens_2026.json"
    json_file.write_text(json.dumps([{"item_id": 10, "qty": 2}]), encoding="utf-8")

    uow = SqlUnitOfWork(get_session_factory())
    secret_manager = NoopSecretManagerAdapter()
    factory = DiscoveryRunnerFactoryImpl(secret_manager=secret_manager)
    use_case = RunDiscoveryUseCase(uow=uow, runner_factory=factory)

    async with uow:
        endpoint = FileSystemEndpoint(
            id="ep-fs-test",
            name="landing-fs",
            credential_ref=CredentialReference("vault/none"),
            root_path=str(tmp_path),
        )

        await uow.endpoints.save(endpoint)

        asset = DataAsset(
            id="asset-fs-test",
            name="landing-asset",
            description="Landing folder",
            owner=EmailAddress("data@co.com"),
            state=AssetState.ACTIVE,
            endpoint_id="ep-fs-test",
            discovery_schedule=CronSchedule("0 0 * * *"),
            discovery_scope=DiscoveryScope(include=["*vendas*.csv:vendas", "*itens*.json:itens"]),
        )
        await uow.assets.save(asset)
        await uow.commit()

    run = await use_case.execute("asset-fs-test", triggered_by="integration_test")

    assert run.status == DiscoveryRunStatus.COMPLETED
    assert len(run.snapshots) == 2

    async with uow:
        objects = await uow.objects.find_by_asset_id("asset-fs-test")
        assert len(objects) == 2
        obj_names = {o.name for o in objects}
        assert "vendas" in obj_names
        assert "itens" in obj_names


@pytest.mark.asyncio
async def test_trigger_filesystem_discovery_via_http(
    po_pm_client,
    db_session: AsyncSession,
    tmp_path: Path,
) -> None:
    from app.infrastructure.persistence.models.data_asset_model import DataAssetModel
    from app.infrastructure.persistence.models.endpoint_model import EndpointModel

    # Setup file
    csv_file = tmp_path / "clientes.csv"
    csv_file.write_text("id,nome\n1,Carlos\n", encoding="utf-8")

    endpoint = EndpointModel(
        id="ep-fs-http",
        name="fs-http",
        type="file_system",
        credential_ref="vault/none",
        technical_description="",
        subtype_data={"root_path": str(tmp_path)},
    )
    db_session.add(endpoint)

    asset = DataAssetModel(
        id="asset-fs-http",
        name="clientes-landing",
        description="Clientes",
        owner_email="data@co.com",
        state=AssetState.ACTIVE.value,
        discovery_schedule="0 * * * *",
        discovery_scope={"include": ["*clientes*.csv:clientes"], "exclude": []},
        tags=[],
        policy_tags=[],
        endpoint_id="ep-fs-http",
    )
    db_session.add(asset)
    await db_session.commit()

    response = await po_pm_client.post(
        "/v1/discovery/assets/clientes-landing/run", json={"triggered_by": "http_test"}
    )
    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "completed"
    assert data["asset_id"] == "asset-fs-http"
