from __future__ import annotations

from unittest.mock import MagicMock

from app.application.pipelines.services.pipeline_validator import PipelineValidator
from app.infrastructure.dag_generator.dag_generator import DagGenerator


def test_pipeline_validator_valid_yaml():
    mock_dag_gen = MagicMock(spec=DagGenerator)
    mock_dag_gen.generate.return_value = "# dag code"

    yaml_content = "pipeline_id: test_pipe\ntype: ingestion\nsource_query: SELECT * FROM users"
    validator = PipelineValidator(dag_generator=mock_dag_gen)
    response = validator.validate(yaml_content, pipeline_type="ingestion")

    assert response.is_valid
    assert len(response.errors) == 0


def test_pipeline_validator_missing_id():
    mock_dag_gen = MagicMock(spec=DagGenerator)
    yaml_content = "type: ingestion"
    validator = PipelineValidator(dag_generator=mock_dag_gen)
    response = validator.validate(yaml_content)

    assert not response.is_valid
    assert response.errors[0].error_code == "MISSING_ID"


def test_pipeline_validator_invalid_sql():
    mock_dag_gen = MagicMock(spec=DagGenerator)
    yaml_content = "pipeline_id: test_pipe\ntype: ingestion\nsource_query: SELECT FROM WHERE"
    validator = PipelineValidator(dag_generator=mock_dag_gen)
    response = validator.validate(yaml_content)

    assert not response.is_valid
    assert response.errors[0].error_code == "INVALID_SQL"
