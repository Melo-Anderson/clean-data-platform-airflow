# tests/unit/application/test_get_pipeline_use_case.py
from __future__ import annotations

import pytest

from app.application.pipelines.get_pipeline_use_case import GetPipelineUseCase
from app.domain.shared.exceptions import PlatformNotFoundError
from tests.unit.fakes import FakeUnitOfWork, build_fake_pipeline


@pytest.mark.asyncio
async def test_get_pipeline_returns_pipeline_when_found() -> None:
    uow = FakeUnitOfWork()
    pipeline = build_fake_pipeline(name="my-pipeline")
    await uow.pipelines.save(pipeline)
    use_case = GetPipelineUseCase(uow=uow)
    result = await use_case.execute(pipeline.id)
    assert result.id == pipeline.id
    assert result.name == "my-pipeline"


@pytest.mark.asyncio
async def test_get_pipeline_raises_not_found_when_missing() -> None:
    uow = FakeUnitOfWork()
    use_case = GetPipelineUseCase(uow=uow)
    with pytest.raises(PlatformNotFoundError, match="Pipeline not found"):
        await use_case.execute("nonexistent-id")
