from __future__ import annotations

from unittest.mock import MagicMock

from app.application.pipelines.services.pipeline_validator import PipelineValidator
from app.infrastructure.dag_generator.dag_generator import DagGenerator


def test_pipeline_validator_valid_yaml():
    mock_dag_gen = MagicMock(spec=DagGenerator)
    mock_dag_gen.generate.return_value = "# dag code"

    yaml_content = """
name: test_pipe
pipeline_type: ingestion
owner_email: eng@company.com
cron_schedule: "0 6 * * *"
source_asset: postgres_prod
source_objects:
  - object_id: users
    load_strategy: full_load
source_query: SELECT * FROM users
"""
    validator = PipelineValidator(dag_generator=mock_dag_gen)
    response = validator.validate(
        yaml_content, pipeline_type="ingestion", endpoint_type="relational"
    )

    assert response.is_valid
    assert len(response.errors) == 0


def test_pipeline_validator_missing_required_fields():
    mock_dag_gen = MagicMock(spec=DagGenerator)
    yaml_content = "name: test_pipe\npipeline_type: ingestion"
    validator = PipelineValidator(dag_generator=mock_dag_gen)
    response = validator.validate(
        yaml_content, pipeline_type="ingestion", endpoint_type="relational"
    )

    assert not response.is_valid
    assert any(err.error_code == "MISSING_OR_INVALID_FIELD" for err in response.errors)
    assert any(err.json_pointer == "/source_asset" for err in response.errors)


def test_pipeline_validator_invalid_sql():
    mock_dag_gen = MagicMock(spec=DagGenerator)
    yaml_content = """
name: test_pipe
pipeline_type: ingestion
owner_email: eng@company.com
cron_schedule: "0 6 * * *"
source_asset: postgres_prod
source_objects:
  - object_id: users
    load_strategy: full_load
source_query: SELECT FROM WHERE
"""
    validator = PipelineValidator(dag_generator=mock_dag_gen)
    response = validator.validate(
        yaml_content, pipeline_type="ingestion", endpoint_type="relational"
    )

    assert not response.is_valid
    assert response.errors[0].error_code == "INVALID_SQL"
