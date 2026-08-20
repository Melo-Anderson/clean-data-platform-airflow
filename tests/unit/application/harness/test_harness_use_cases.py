from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.application.harness.get_harness_gold_examples import GetHarnessGoldExamplesUseCase
from app.application.harness.get_harness_schema import GetHarnessSchemaUseCase
from app.application.harness.validate_harness_pipeline import ValidateHarnessPipelineUseCase
from app.infrastructure.dag_generator.dag_generator import DagGenerator
from app.infrastructure.providers.pydantic_schema_provider import PydanticSchemaProvider
from app.infrastructure.validators.pydantic_pipeline_validator import PydanticPipelineValidator


@pytest.mark.asyncio
async def test_validate_harness_pipeline_use_case():
    mock_dag_gen = MagicMock(spec=DagGenerator)
    mock_dag_gen.generate.return_value = "# dag code"
    validator = PydanticPipelineValidator(dag_generator=mock_dag_gen)
    use_case = ValidateHarnessPipelineUseCase(validator=validator)
    valid_yaml = """
name: valid_pipe
pipeline_type: ingestion
owner_email: eng@company.com
cron_schedule: "0 6 * * *"
source_asset: postgres_prod
source_objects:
  - object_id: users
    load_strategy: full_load
"""
    res = await use_case.execute(
        pipeline_yaml=valid_yaml, pipeline_type="ingestion", endpoint_type="relational"
    )
    assert res.is_valid


@pytest.mark.asyncio
async def test_get_harness_schema_relational_required():
    use_case = GetHarnessSchemaUseCase(schema_provider=PydanticSchemaProvider())
    result = await use_case.execute(pipeline_type="ingestion", endpoint_type="relational")

    required = result.get("required", [])
    assert "source_asset" in required
    assert "source_objects" in required


@pytest.mark.asyncio
async def test_get_harness_gold_examples_use_case():
    use_case = GetHarnessGoldExamplesUseCase()
    res = await use_case.execute(pipeline_type="ingestion")
    assert res["pipeline_type"] == "ingestion"
    assert res["total_count"] >= 1
    assert len(res["examples"]) >= 1
