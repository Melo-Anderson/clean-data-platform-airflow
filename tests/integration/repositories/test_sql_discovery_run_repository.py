# tests/integration/repositories/test_sql_discovery_run_repository.py
from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.discovery.discovery_run import DiscoveryRun
from app.domain.discovery.discovery_run_status import DiscoveryRunStatus
from app.infrastructure.persistence.repositories.sql_discovery_run_repository import (
    SqlDiscoveryRunRepository,
)


def _run(status: DiscoveryRunStatus = DiscoveryRunStatus.RUNNING) -> DiscoveryRun:
    return DiscoveryRun(
        id=str(uuid.uuid4()),
        asset_id=f"asset_{uuid.uuid4().hex[:6]}",
        triggered_by="test_user@co.com",
        status=status,
        started_at=datetime.now(tz=UTC),
        snapshots=[],
        drift_events=[],
        policy_tag_suggestions=[],
    )


@pytest.mark.asyncio
async def test_save_new_discovery_run(db_session: AsyncSession) -> None:
    repo = SqlDiscoveryRunRepository(db_session)
    run = await repo.save(_run())
    found = await repo.find_by_id(run.id)
    assert found is not None
    assert found.status == DiscoveryRunStatus.RUNNING


@pytest.mark.asyncio
async def test_save_updates_existing_run(db_session: AsyncSession) -> None:
    repo = SqlDiscoveryRunRepository(db_session)
    run = await repo.save(_run())

    run.status = DiscoveryRunStatus.COMPLETED
    updated = await repo.save(run)

    found = await repo.find_by_id(run.id)
    assert found is not None
    assert found.status == DiscoveryRunStatus.COMPLETED
    assert updated.id == run.id


@pytest.mark.asyncio
async def test_find_by_id_returns_none_when_missing(db_session: AsyncSession) -> None:
    repo = SqlDiscoveryRunRepository(db_session)
    found = await repo.find_by_id(str(uuid.uuid4()))
    assert found is None


@pytest.mark.asyncio
async def test_find_latest_by_asset_id_returns_most_recent(db_session: AsyncSession) -> None:
    repo = SqlDiscoveryRunRepository(db_session)

    run1 = _run(status=DiscoveryRunStatus.COMPLETED)
    run1.completed_at = datetime.now(tz=UTC) - timedelta(days=1)
    await repo.save(run1)

    run2 = _run(status=DiscoveryRunStatus.COMPLETED)
    run2.asset_id = run1.asset_id
    run2.completed_at = datetime.now(tz=UTC)
    await repo.save(run2)

    found = await repo.find_latest_by_asset_id(run1.asset_id)
    assert found is not None
    assert found.id == run2.id


@pytest.mark.asyncio
async def test_find_all_by_asset_id_respects_limit(db_session: AsyncSession) -> None:
    repo = SqlDiscoveryRunRepository(db_session)

    asset_id = "asset_limit_test"
    for _ in range(5):
        run = _run()
        run.asset_id = asset_id
        await repo.save(run)

    found = await repo.find_all_by_asset_id(asset_id, limit=3)
    assert len(found) == 3
