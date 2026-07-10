from unittest.mock import AsyncMock, MagicMock

import pytest

from app.domain.pipelines.pipeline import Pipeline
from app.domain.pipelines.pipeline_type import PipelineType
from app.domain.pipelines.schedule_config import ScheduleConfig
from app.domain.pipelines.schedule_mode import ScheduleMode
from app.domain.shared.value_objects import CronSchedule, EmailAddress
from app.infrastructure.persistence.repositories.sql_pipeline_repository import (
    SqlPipelineRepository,
)


def make_pipeline() -> Pipeline:
    return Pipeline(
        id="pipe-001",
        name="ingest-e2e-asset",
        type=PipelineType.INGESTION,
        owner=EmailAddress("e2e@co.com"),
        schedule=ScheduleConfig(
            mode=ScheduleMode.CRON,
            cron_schedule=CronSchedule("0 0 * * *"),
        ),
        source_asset_id="asset-001",
        schema_version="1.0",
    )


@pytest.mark.asyncio
async def test_save_returns_pipeline_with_id() -> None:
    session = MagicMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    repo = SqlPipelineRepository(session)
    p = make_pipeline()
    result = await repo.save(p)
    assert result.id == "pipe-001"
    assert result.name == "ingest-e2e-asset"
    session.add.assert_called_once()
