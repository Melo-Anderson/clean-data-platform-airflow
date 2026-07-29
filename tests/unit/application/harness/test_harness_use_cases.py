from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.application.harness.get_harness_gold_examples import GetHarnessGoldExamplesUseCase
from app.application.harness.get_harness_schema import GetHarnessSchemaUseCase
from app.application.harness.validate_harness_pipeline import ValidateHarnessPipelineUseCase
from app.application.pipelines.services.pipeline_validator import PipelineValidator
from app.infrastructure.dag_generator.dag_generator import DagGenerator


@pytest.mark.asyncio
async def test_validate_harness_pipeline_use_case():
    mock_dag_gen = MagicMock(spec=DagGenerator)
    validator = PipelineValidator(dag_generator=mock_dag_gen)
    use_case = ValidateHarnessPipelineUseCase(validator=validator)
    res = await use_case.execute(
        pipeline_yaml="pipeline_id: valid\ntype: ingestion", pipeline_type="relational"
    )
    assert res.is_valid


@pytest.mark.asyncio
async def test_get_harness_schema_use_case():
    use_case = GetHarnessSchemaUseCase()
    res = await use_case.execute(pipeline_type="all")
    assert res.type == "object"


@pytest.mark.asyncio
async def test_get_harness_gold_examples_use_case():
    use_case = GetHarnessGoldExamplesUseCase()
    res = await use_case.execute(pipeline_type="all")
    assert len(res.examples) > 0
