from __future__ import annotations

from app.infrastructure.dag_generator.ci_validator import CiValidator


def test_ci_validator_accepts_valid_sensor_timeout() -> None:
    yaml_content = """
pipeline:
  airflow:
    execution_timeout_minutes: 120
  source:
    objects:
      - object_id: obj-1
        sensor:
          query: "SELECT 1"
          timeout_minutes: 60
    """
    validator = CiValidator()
    errors = validator.validate_yaml(yaml_content)
    assert not errors


def test_ci_validator_rejects_sensor_timeout_greater_than_execution_timeout() -> None:
    yaml_content = """
pipeline:
  airflow:
    execution_timeout_minutes: 60
  source:
    objects:
      - object_id: obj-1
        sensor:
          query: "SELECT 1"
          timeout_minutes: 120
    """
    validator = CiValidator()
    errors = validator.validate_yaml(yaml_content)
    assert len(errors) == 1
    assert "timeout_minutes (120) > execution_timeout_minutes (60)" in errors[0]


def test_ci_validator_invalid_yaml() -> None:
    validator = CiValidator()
    errors = validator.validate_yaml("{ invalid yaml")
    assert len(errors) == 1
    assert "Invalid YAML" in errors[0]


def test_ci_validator_missing_pipeline_key() -> None:
    validator = CiValidator()
    errors = validator.validate_yaml("not_pipeline: true")
    assert len(errors) == 1
    assert "YAML must contain a 'pipeline' root key." in errors[0]
