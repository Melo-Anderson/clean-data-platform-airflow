# tests/integration/repositories/test_sql_pipeline_run_repository.py
from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.pipelines.pipeline_run import PipelineRun
from app.domain.pipelines.pipeline_run_status import PipelineRunStatus
from app.infrastructure.persistence.models.pipeline_run_model import PipelineRunModel
from app.infrastructure.persistence.repositories.sql_pipeline_run_repository import (
    SqlPipelineRunRepository,
)


def _run(status: PipelineRunStatus = PipelineRunStatus.RUNNING) -> PipelineRun:
    return PipelineRun(
        id=str(uuid.uuid4()),
        pipeline_id=f"pipe_{uuid.uuid4().hex[:6]}",
        pipeline_name="test_pipeline",
        pipeline_type="ingestion",
        dag_run_id="dag_run_1",
        status=status,
        started_at=datetime.now(tz=UTC),
        sla_minutes=10,
    )


@pytest.mark.asyncio
async def test_save_new_pipeline_run(db_session: AsyncSession) -> None:
    repo = SqlPipelineRunRepository(db_session)
    run = await repo.save(_run())
    found = await repo.find_by_id(run.id)
    assert found is not None
    assert found.status == PipelineRunStatus.RUNNING


@pytest.mark.asyncio
async def test_save_updates_existing_run(db_session: AsyncSession) -> None:
    repo = SqlPipelineRunRepository(db_session)
    run = await repo.save(_run())

    run.status = PipelineRunStatus.SUCCESS
    updated = await repo.save(run)

    found = await repo.find_by_id(run.id)
    assert found is not None
    assert found.status == PipelineRunStatus.SUCCESS
    assert updated.id == run.id


@pytest.mark.asyncio
async def test_save_sets_last_success_at_on_success(db_session: AsyncSession) -> None:
    repo = SqlPipelineRunRepository(db_session)
    run = await repo.save(_run(status=PipelineRunStatus.SUCCESS))

    model = await db_session.get(PipelineRunModel, run.id)
    assert model is not None
    assert model.last_success_at is not None


@pytest.mark.asyncio
async def test_save_does_not_set_last_success_at_on_failure(db_session: AsyncSession) -> None:
    repo = SqlPipelineRunRepository(db_session)
    run = await repo.save(_run(status=PipelineRunStatus.FAILED))

    model = await db_session.get(PipelineRunModel, run.id)
    assert model is not None
    assert model.last_success_at is None


@pytest.mark.asyncio
async def test_find_by_id_returns_run(db_session: AsyncSession) -> None:
    repo = SqlPipelineRunRepository(db_session)
    run = await repo.save(_run())
    found = await repo.find_by_id(run.id)
    assert found is not None
    assert found.id == run.id


@pytest.mark.asyncio
async def test_find_by_id_returns_none_when_missing(db_session: AsyncSession) -> None:
    repo = SqlPipelineRunRepository(db_session)
    found = await repo.find_by_id(str(uuid.uuid4()))
    assert found is None


@pytest.mark.asyncio
async def test_find_latest_by_pipeline_id(db_session: AsyncSession) -> None:
    repo = SqlPipelineRunRepository(db_session)

    run1 = _run()
    run1.started_at = datetime.now(tz=UTC) - timedelta(days=1)
    await repo.save(run1)

    run2 = _run()
    run2.pipeline_id = run1.pipeline_id
    run2.started_at = datetime.now(tz=UTC)
    await repo.save(run2)

    found = await repo.find_latest_by_pipeline_id(run1.pipeline_id)
    assert found is not None
    assert found.id == run2.id


@pytest.mark.asyncio
async def test_find_dashboard_summary(db_session: AsyncSession) -> None:
    repo = SqlPipelineRunRepository(db_session)
    run1 = await repo.save(_run())

    summary = await repo.find_dashboard_summary()
    assert len(summary) >= 1

    item = next((i for i in summary if i["pipeline_id"] == run1.pipeline_id), None)
    assert item is not None
    assert item["status"] == run1.status.value
    assert item["pipeline_name"] == run1.pipeline_name
    assert "last_run_at" in item
