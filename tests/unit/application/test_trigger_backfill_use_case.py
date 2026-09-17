from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.application.pipelines.trigger_backfill_use_case import TriggerBackfillUseCase
from app.domain.shared.exceptions import PlatformNotFoundError
from tests.unit.fakes import FakeUnitOfWork, build_fake_pipeline


@pytest.mark.asyncio
async def test_trigger_backfill_returns_backfill_id() -> None:
    uow = FakeUnitOfWork()
    pipeline = build_fake_pipeline(name="my-ingestion-pipeline")
    await uow.pipelines.save(pipeline)

    adapter = MagicMock()
    adapter.create_backfill = AsyncMock(return_value="backfill-123")

    use_case = TriggerBackfillUseCase(uow=uow, backfill_adapter=adapter)
    result = await use_case.execute(
        pipeline_id=pipeline.id,
        from_date=date(2026, 1, 1),
        to_date=date(2026, 1, 7),
    )
    assert result == "backfill-123"
    adapter.create_backfill.assert_called_once_with(
        dag_id="my-ingestion-pipeline",
        from_date=date(2026, 1, 1),
        to_date=date(2026, 1, 7),
    )


@pytest.mark.asyncio
async def test_trigger_backfill_raises_not_found_when_pipeline_missing() -> None:
    uow = FakeUnitOfWork()
    adapter = MagicMock()
    use_case = TriggerBackfillUseCase(uow=uow, backfill_adapter=adapter)
    with pytest.raises(PlatformNotFoundError, match="Pipeline not found"):
        await use_case.execute(
            pipeline_id="nonexistent",
            from_date=date(2026, 1, 1),
            to_date=date(2026, 1, 7),
        )
