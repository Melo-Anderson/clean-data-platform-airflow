from __future__ import annotations

import pytest

from app.domain.pipelines.pipeline import Pipeline
from app.domain.pipelines.pipeline_type import PipelineType
from app.domain.pipelines.schedule_config import CronSchedule, ScheduleConfig, ScheduleMode
from app.domain.shared.value_objects import EmailAddress
from tests.unit.mocks.named_mocks import MockNamedUnitOfWork


@pytest.mark.asyncio
async def test_mock_named_unit_of_work_lifecycle() -> None:
    uow = MockNamedUnitOfWork()
    pipe = Pipeline(
        id="pipe-001",
        name="test_pipe",
        type=PipelineType.INGESTION,
        owner=EmailAddress("dev@co.com"),
        schedule=ScheduleConfig(mode=ScheduleMode.CRON, cron_schedule=CronSchedule("0 0 * * *")),
    )
    async with uow:
        await uow.pipelines.save(pipe)
        await uow.commit()

    assert uow.committed
    retrieved = await uow.pipelines.find_by_id("pipe-001")
    assert retrieved is not None
    assert retrieved.name == "test_pipe"
