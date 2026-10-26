from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.pipelines.pipeline_run_status import PipelineRunStatus
from app.infrastructure.persistence.models.pipeline_model import PipelineModel
from app.infrastructure.persistence.models.pipeline_run_model import PipelineRunModel


@pytest.mark.asyncio
async def test_get_watermark_returns_watermark_when_exists(
    ae_client: AsyncClient, db_session: AsyncSession
) -> None:
    pipeline_id = str(uuid.uuid4())
    pipe = PipelineModel(
        id=pipeline_id,
        name="pipe_watermark_test",
        type="ingestion",
        owner_email="eng@example.com",
        schedule={"mode": "cron", "cron_schedule": "0 0 * * *"},
        source_asset_name="src",
        destination_asset_name="dst",
        schema_version="1.0",
        source_objects=[],
        destination_objects=[],
        transform={"engine": "none", "ref": None},
        compute={"engine": "duckdb", "num_workers": 1},
        quality_rules=[],
        airflow={},
        discovery_task={},
    )
    db_session.add(pipe)

    run = PipelineRunModel(
        id=str(uuid.uuid4()),
        pipeline_id=pipeline_id,
        pipeline_name="pipe_watermark_test",
        pipeline_type="ingestion",
        dag_run_id="run_1",
        status=PipelineRunStatus.SUCCESS.value,
        started_at=datetime(2026, 10, 24, 10, 0, 0, tzinfo=UTC),
        last_run_at=datetime(2026, 10, 24, 10, 0, 0, tzinfo=UTC),
        watermark_value="2026-10-24T10:00:00",
        watermark_type="timestamp",
    )
    db_session.add(run)
    await db_session.commit()

    resp = await ae_client.get(f"/v1/pipelines/{pipeline_id}/watermark")
    assert resp.status_code == 200
    data = resp.json()
    assert data == {"value": "2026-10-24T10:00:00", "type": "timestamp"}


@pytest.mark.asyncio
async def test_get_watermark_returns_null_when_none(
    ae_client: AsyncClient, db_session: AsyncSession
) -> None:
    pipeline_id = str(uuid.uuid4())
    pipe = PipelineModel(
        id=pipeline_id,
        name="pipe_empty_watermark",
        type="ingestion",
        owner_email="eng@example.com",
        schedule={"mode": "cron", "cron_schedule": "0 0 * * *"},
        source_asset_name="src",
        destination_asset_name="dst",
        schema_version="1.0",
        source_objects=[],
        destination_objects=[],
        transform={"engine": "none", "ref": None},
        compute={"engine": "duckdb", "num_workers": 1},
        quality_rules=[],
        airflow={},
        discovery_task={},
    )
    db_session.add(pipe)
    await db_session.commit()

    resp = await ae_client.get(f"/v1/pipelines/{pipeline_id}/watermark")
    assert resp.status_code == 200
    assert resp.json() is None
