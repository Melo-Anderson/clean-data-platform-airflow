# tests/unit/application/test_list_pipelines_use_case.py
from __future__ import annotations

import pytest

from app.application.pipelines.list_pipelines_use_case import ListPipelinesUseCase
from tests.unit.fakes import FakeUnitOfWork, build_fake_pipeline


@pytest.mark.asyncio
async def test_list_pipelines_returns_empty_when_no_pipelines() -> None:
    uow = FakeUnitOfWork()
    use_case = ListPipelinesUseCase(uow=uow)
    result = await use_case.execute()
    assert result == []


@pytest.mark.asyncio
async def test_list_pipelines_returns_all_saved_pipelines() -> None:
    uow = FakeUnitOfWork()
    p1 = build_fake_pipeline(name="pipeline-a")
    p2 = build_fake_pipeline(name="pipeline-b")
    await uow.pipelines.save(p1)
    await uow.pipelines.save(p2)
    use_case = ListPipelinesUseCase(uow=uow)
    result = await use_case.execute()
    assert len(result) == 2
    assert {p.name for p in result} == {"pipeline-a", "pipeline-b"}
