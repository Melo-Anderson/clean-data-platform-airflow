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
        "/v1/pipelines/",
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
        "/v1/pipelines/",
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
    response = await ae_client.get(f"/v1/pipelines/{uuid.uuid4()}")
    assert response.status_code == 404
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_pipeline_returns_pipeline(ae_client: AsyncClient, client: AsyncClient) -> None:
    create_resp = await ae_client.post(
        "/v1/pipelines/",
        json={
            "name": "get_test",
            "pipeline_type": "etl",
            "owner_email": "owner@co.com",
            "source_asset_id": str(uuid.uuid4()),
            "cron_schedule": "0 1 * * *",
        },
    )
    pipeline_id = create_resp.json()["id"]

    get_resp = await ae_client.get(f"/v1/pipelines/{pipeline_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["id"] == pipeline_id
    assert get_resp.json()["name"] == "get_test"


@pytest.mark.asyncio
async def test_report_quality_gate_returns_404_for_unknown_run(ae_client: AsyncClient) -> None:
    response = await ae_client.post(
        f"/v1/pipelines/{uuid.uuid4()}/runs/{uuid.uuid4()}/quality-gate",
        json={"metrics": {"rows": 10}},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_report_quality_gate_returns_200_on_success(
    ae_client: AsyncClient, client: AsyncClient, db_session
) -> None:
    create_resp = await ae_client.post(
        "/v1/pipelines/",
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
        f"/v1/pipelines/{pipeline_id}/runs/{run_id}/quality-gate",
        json={"metrics": {"row_count": 100}},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert data["run_id"] == run_id


@pytest.mark.asyncio
async def test_router_serializes_source_objects_for_use_case() -> None:
    """Router deve serializar source_objects via .model_dump() antes de passar ao use case."""
    from unittest.mock import AsyncMock, MagicMock, patch

    from app.domain.pipelines.pipeline import Pipeline
    from app.domain.pipelines.pipeline_type import PipelineType
    from app.domain.pipelines.schedule_config import ScheduleConfig
    from app.domain.pipelines.schedule_mode import ScheduleMode
    from app.domain.shared.value_objects import CronSchedule, EmailAddress
    from app.infrastructure.http.schemas.pipeline_schemas import (
        CreatePipelineRequest,
        ExtractionObjectRequest,
    )

    body = CreatePipelineRequest(
        name="test_pipe",
        pipeline_type="ingestion",
        owner_email="eng@co.com",
        source_asset_id="asset-001",
        cron_schedule="0 * * * *",
        source_objects=[
            ExtractionObjectRequest(
                object_id="demo_orders",
                extraction_query="SELECT id FROM demo_orders",
            )
        ],
    )

    returned_pipeline = Pipeline(
        id="pipe-xx",
        name="test_pipe",
        type=PipelineType.INGESTION,
        owner=EmailAddress("eng@co.com"),
        schedule=ScheduleConfig(mode=ScheduleMode.CRON, cron_schedule=CronSchedule("0 * * * *")),
        source_asset_id="asset-001",
        schema_version="1.0",
    )

    with patch(
        "app.infrastructure.http.routers.pipeline_router.RegisterPipelineUseCase"
    ) as MockUseCase:
        mock_uc = MagicMock()
        mock_uc.execute = AsyncMock(return_value=returned_pipeline)
        MockUseCase.return_value = mock_uc

        source_objs_raw = (
            [o.model_dump() for o in body.source_objects] if body.source_objects else None
        )
        await mock_uc.execute(
            name=body.name,
            pipeline_type=body.pipeline_type,
            owner_email=body.owner_email,
            source_asset_id=body.source_asset_id,
            cron_schedule=body.cron_schedule,
            destination_asset_id=body.destination_asset_id or "",
            destination_objects=body.destination_objects,
            source_objects=source_objs_raw,
            compute=body.compute.model_dump() if body.compute else None,
            quality_rules=[r.model_dump() for r in body.quality_rules]
            if body.quality_rules
            else None,
            airflow_config=body.airflow_config.model_dump() if body.airflow_config else None,
        )

        call_kwargs = mock_uc.execute.call_args.kwargs
        assert call_kwargs["source_objects"] == [
            {
                "object_id": "demo_orders",
                "load_strategy": "full_load",
                "watermark_column": None,
                "page_size": 1000,
                "partition_column": None,
                "compression": "snappy",
                "encoding": "utf-8",
                "extraction_query": "SELECT id FROM demo_orders",
            }
        ]
