# tests/unit/infrastructure/http/routers/test_pipeline_router.py
from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from httpx import AsyncClient

from app.domain.pipelines.pipeline_run_status import PipelineRunStatus
from app.infrastructure.persistence.models.pipeline_run_model import PipelineRunModel


@pytest.mark.asyncio
async def test_register_pipeline_returns_201(ae_client: AsyncClient) -> None:
    response = await ae_client.post(
        "/pipelines/",
        json={
            "name": "test_pipeline",
            "pipeline_type": "ingestion",
            "owner_email": "test@co.com",
            "source_asset_id": str(uuid.uuid4()),
            "cron_schedule": "0 12 * * *",
        },
    )
    assert response.status_code == 201
    data = response.json()
    assert data["name"] == "test_pipeline"
    assert data["cron_schedule"] == "0 12 * * *"
    assert "id" in data


@pytest.mark.asyncio
async def test_register_pipeline_invalid_cron_returns_422(ae_client: AsyncClient) -> None:
    response = await ae_client.post(
        "/pipelines/",
        json={
            "name": "test_pipeline",
            "pipeline_type": "ingestion",
            "owner_email": "test@co.com",
            "source_asset_id": str(uuid.uuid4()),
            "cron_schedule": "invalid_cron",
        },
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_get_pipeline_returns_404_when_not_found(ae_client: AsyncClient) -> None:
    response = await ae_client.get(f"/pipelines/{uuid.uuid4()}")
    assert response.status_code == 404
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_pipeline_returns_pipeline(ae_client: AsyncClient, client: AsyncClient) -> None:
    create_resp = await ae_client.post(
        "/pipelines/",
        json={
            "name": "get_test",
            "pipeline_type": "etl",
            "owner_email": "owner@co.com",
            "source_asset_id": str(uuid.uuid4()),
            "cron_schedule": "0 1 * * *",
        },
    )
    pipeline_id = create_resp.json()["id"]

    get_resp = await ae_client.get(f"/pipelines/{pipeline_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["id"] == pipeline_id
    assert get_resp.json()["name"] == "get_test"


@pytest.mark.asyncio
async def test_report_quality_gate_returns_404_for_unknown_run(ae_client: AsyncClient) -> None:
    response = await ae_client.post(
        f"/pipelines/{uuid.uuid4()}/runs/{uuid.uuid4()}/quality-gate",
        json={"metrics": {"rows": 10}},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_report_quality_gate_returns_200_on_success(
    ae_client: AsyncClient, client: AsyncClient, db_session
) -> None:
    create_resp = await ae_client.post(
        "/pipelines/",
        json={
            "name": "qg_test",
            "pipeline_type": "etl",
            "owner_email": "owner@co.com",
            "source_asset_id": str(uuid.uuid4()),
            "cron_schedule": "0 1 * * *",
        },
    )
    pipeline_id = create_resp.json()["id"]

    run_id = str(uuid.uuid4())
    model = PipelineRunModel(
        id=run_id,
        pipeline_id=pipeline_id,
        pipeline_name="qg_test",
        pipeline_type="etl",
        dag_run_id="dag1",
        status=PipelineRunStatus.RUNNING.value,
        started_at=datetime.now(tz=UTC),
        last_run_at=datetime.now(tz=UTC),
    )
    db_session.add(model)
    await db_session.commit()

    response = await ae_client.post(
        f"/pipelines/{pipeline_id}/runs/{run_id}/quality-gate",
        json={"metrics": {"row_count": 100}},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert data["run_id"] == run_id
