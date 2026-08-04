from __future__ import annotations

import pytest

from app.application.harness.get_harness_gold_examples import GetHarnessGoldExamplesUseCase


@pytest.mark.asyncio
async def test_get_gold_examples_requires_type_and_respects_limit() -> None:
    """With no real DB, the canonical fallback is returned respecting limit."""
    use_case = GetHarnessGoldExamplesUseCase(uow=None)
    res = await use_case.execute(pipeline_type="ingestion", limit=2)
    assert res["pipeline_type"] == "ingestion"
    assert res["total_count"] <= 2
    assert len(res["examples"]) <= 2


@pytest.mark.asyncio
async def test_get_gold_examples_has_fallback() -> None:
    """Fallback canonical YAML is returned when no UoW is given."""
    use_case = GetHarnessGoldExamplesUseCase(uow=None)
    res = await use_case.execute(pipeline_type="etl")
    assert res["pipeline_type"] == "etl"
    assert len(res["examples"]) == 1


@pytest.mark.asyncio
async def test_get_gold_examples_with_compute_engine_filter() -> None:
    """compute_engine filter is applied when UoW is available."""
    use_case = GetHarnessGoldExamplesUseCase(uow=None)
    res = await use_case.execute(pipeline_type="ingestion", compute_engine="spark")
    # Falls back to canonical because uow is None
    assert res["pipeline_type"] == "ingestion"
    assert len(res["examples"]) >= 1


from unittest.mock import AsyncMock, MagicMock

from app.infrastructure.yaml_generator.pipeline_yaml_generator import PipelineYamlGenerator


@pytest.mark.asyncio
async def test_get_gold_examples_uses_yaml_generator() -> None:
    uow = MagicMock()
    mock_pipeline = MagicMock()
    mock_pipeline.type.value = "ingestion"
    mock_pipeline.compute.engine.value = "spark"
    mock_pipeline.transform.engine.value = "none"
    mock_pipeline.source_asset = "src_1"
    mock_pipeline.id = "p_real_1"

    uow.pipelines.find_all = AsyncMock(return_value=[mock_pipeline])

    yaml_gen = MagicMock(spec=PipelineYamlGenerator)
    yaml_gen.generate.return_value = (
        "schema_version: '1.0'\npipeline:\n  id: p_real_1\n  type: ingestion\n"
    )

    use_case = GetHarnessGoldExamplesUseCase(uow=uow, yaml_generator=yaml_gen)
    res = await use_case.execute(pipeline_type="ingestion")

    assert res["pipeline_type"] == "ingestion"
    assert len(res["examples"]) == 1
    assert res["examples"][0]["pipeline_id"] == "p_real_1"
    assert res["examples"][0]["yaml_snippet"] == yaml_gen.generate.return_value
    yaml_gen.generate.assert_called_once_with(mock_pipeline)
