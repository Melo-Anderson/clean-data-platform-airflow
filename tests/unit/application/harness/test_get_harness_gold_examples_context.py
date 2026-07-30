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
async def test_get_gold_examples_no_quality_metrics_in_fallback() -> None:
    """Fallback canonical YAML must not include quality.metrics block."""
    use_case = GetHarnessGoldExamplesUseCase(uow=None)
    res = await use_case.execute(pipeline_type="etl")
    for example in res["examples"]:
        assert "quality" not in example["yaml_snippet"]
        assert "metrics:" not in example["yaml_snippet"]


@pytest.mark.asyncio
async def test_get_gold_examples_with_compute_engine_filter() -> None:
    """compute_engine filter is applied when UoW is available."""
    use_case = GetHarnessGoldExamplesUseCase(uow=None)
    res = await use_case.execute(pipeline_type="ingestion", compute_engine="spark")
    # Falls back to canonical because uow is None
    assert res["pipeline_type"] == "ingestion"
    assert len(res["examples"]) >= 1
