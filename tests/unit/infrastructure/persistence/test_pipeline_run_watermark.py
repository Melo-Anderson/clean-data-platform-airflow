from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.domain.pipelines.pipeline_run import PipelineRun
from app.domain.pipelines.pipeline_run_status import PipelineRunStatus
from app.domain.pipelines.watermark import Watermark, WatermarkType
from app.infrastructure.persistence.base_model import Base
from app.infrastructure.persistence.models.pipeline_model import PipelineModel
from app.infrastructure.persistence.repositories.sql_pipeline_run_repository import (
    SqlPipelineRunRepository,
)


@pytest.fixture
async def async_session() -> AsyncSession:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        # Create pipeline record first for foreign key
        pipe = PipelineModel(
            id="p-1",
            name="test_pipeline",
            type="ingestion",
            owner_email="owner@example.com",
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
        session.add(pipe)
        await session.commit()
        yield session

    await engine.dispose()


@pytest.mark.asyncio
async def test_save_and_find_latest_watermark(async_session: AsyncSession) -> None:
    repo = SqlPipelineRunRepository(async_session)

    run1 = PipelineRun(
        id="run-1",
        pipeline_id="p-1",
        pipeline_name="test_pipeline",
        pipeline_type="ingestion",
        dag_run_id="run_1",
        status=PipelineRunStatus.SUCCESS,
        started_at=datetime(2026, 10, 24, 10, 0, 0, tzinfo=UTC),
        watermark=Watermark("100", WatermarkType.INTEGER),
    )
    await repo.save(run1)

    run2 = PipelineRun(
        id="run-2",
        pipeline_id="p-1",
        pipeline_name="test_pipeline",
        pipeline_type="ingestion",
        dag_run_id="run_2",
        status=PipelineRunStatus.SUCCESS,
        started_at=datetime(2026, 10, 24, 11, 0, 0, tzinfo=UTC),
        watermark=Watermark("200", WatermarkType.INTEGER),
    )
    await repo.save(run2)

    latest_wm = await repo.find_latest_watermark_by_pipeline_id("p-1")
    assert latest_wm is not None
    assert latest_wm.value == "200"
    assert latest_wm.type == WatermarkType.INTEGER


@pytest.mark.asyncio
async def test_latest_watermark_ignores_failed_runs(async_session: AsyncSession) -> None:
    repo = SqlPipelineRunRepository(async_session)

    run1 = PipelineRun(
        id="run-1",
        pipeline_id="p-1",
        pipeline_name="test_pipeline",
        pipeline_type="ingestion",
        dag_run_id="run_1",
        status=PipelineRunStatus.SUCCESS,
        started_at=datetime(2026, 10, 24, 10, 0, 0, tzinfo=UTC),
        watermark=Watermark("100", WatermarkType.INTEGER),
    )
    await repo.save(run1)

    # Failed run with watermark value should be ignored
    run2 = PipelineRun(
        id="run-2",
        pipeline_id="p-1",
        pipeline_name="test_pipeline",
        pipeline_type="ingestion",
        dag_run_id="run_2",
        status=PipelineRunStatus.FAILED,
        started_at=datetime(2026, 10, 24, 12, 0, 0, tzinfo=UTC),
        watermark=Watermark("300", WatermarkType.INTEGER),
    )
    await repo.save(run2)

    latest_wm = await repo.find_latest_watermark_by_pipeline_id("p-1")
    assert latest_wm is not None
    assert latest_wm.value == "100"
