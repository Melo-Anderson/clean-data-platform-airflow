from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.application.harness.get_pipeline_yaml import GetPipelineYamlUseCase


@pytest.mark.asyncio
async def test_get_pipeline_yaml_success() -> None:
    uow = MagicMock()
    mock_pipeline = MagicMock()
    mock_pipeline.id = "p_test"
    uow.pipelines.find_by_id = AsyncMock(return_value=mock_pipeline)
    yaml_gen = MagicMock()
    yaml_gen.generate.return_value = "pipeline:\n  id: p_test\n  type: ingestion\n"

    use_case = GetPipelineYamlUseCase(uow=uow, yaml_generator=yaml_gen)
    res = await use_case.execute(pipeline_id="p_test")

    assert res["pipeline_id"] == "p_test"
    assert "pipeline:" in res["pipeline_yaml"]


@pytest.mark.asyncio
async def test_get_pipeline_yaml_not_found() -> None:
    uow = MagicMock()
    uow.pipelines.find_by_id = AsyncMock(return_value=None)
    use_case = GetPipelineYamlUseCase(uow=uow)

    with pytest.raises(ValueError, match="Pipeline not found"):
        await use_case.execute(pipeline_id="p_missing")
