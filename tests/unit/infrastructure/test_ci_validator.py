from __future__ import annotations

from app.infrastructure.dag_generator.ci_validator import CiValidator


def test_ci_validator_accepts_valid_sensor_timeout() -> None:
    yaml_content = """
pipeline:
  airflow:
    execution_timeout_minutes: 120
  source:
    objects:
      - object_name: obj-1
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
      - object_name: obj-1
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


VALID_CONFIG = {
    "id": "pipe-1",
    "name": "my_pipeline",
    "type": "ingestion",
    "owner": "owner@test.com",
    "schedule": {"mode": "cron", "cron": "0 6 * * *"},
    "source": {
        "asset_name": "src_asset",
        "objects": [{"object_name": "obj1", "load_strategy": "full"}],
    },
    "destination": {"asset_name": "dst_asset", "objects": [{"object_name": "tbl1"}]},
    "compute": {"engine": "duckdb", "staging_bucket": "/tmp/landing"},
    "quality": {"metrics": [{"type": "row_count_min", "threshold": 1}]},
    "airflow": {
        "retries": 3,
        "retry_delay_minutes": 5,
        "execution_timeout_minutes": 120,
        "sla_minutes": 90,
        "pool": "default_pool",
    },
}


def test_ci_validator_accepts_valid_config() -> None:
    import yaml

    yaml_str = yaml.dump({"pipeline": VALID_CONFIG})
    errors = CiValidator().validate_yaml(yaml_str)
    assert errors == []


def test_ci_validator_rejects_missing_owner() -> None:
    import yaml

    cfg = {**VALID_CONFIG, "owner": ""}
    yaml_str = yaml.dump({"pipeline": cfg})
    errors = CiValidator().validate_yaml(yaml_str)
    assert any("owner" in e.lower() for e in errors)


def test_ci_validator_rejects_invalid_schedule_mode() -> None:
    import yaml

    cfg = {**VALID_CONFIG, "schedule": {"mode": "unknown_mode"}}
    yaml_str = yaml.dump({"pipeline": cfg})
    errors = CiValidator().validate_yaml(yaml_str)
    assert any("schedule" in e.lower() for e in errors)


def test_ci_validator_rejects_invalid_cron() -> None:
    import yaml

    cfg = {**VALID_CONFIG, "schedule": {"mode": "cron", "cron": "99 99 * * *"}}
    yaml_str = yaml.dump({"pipeline": cfg})
    errors = CiValidator().validate_yaml(yaml_str)
    assert any("cron" in e.lower() for e in errors)


def test_ci_validator_rejects_negative_retries() -> None:
    import yaml

    cfg = {**VALID_CONFIG, "airflow": {**VALID_CONFIG["airflow"], "retries": -1}}
    yaml_str = yaml.dump({"pipeline": cfg})
    errors = CiValidator().validate_yaml(yaml_str)
    assert any("retries" in e.lower() for e in errors)


def test_ci_validator_rejects_zero_sla_minutes() -> None:
    import yaml

    cfg = {**VALID_CONFIG, "airflow": {**VALID_CONFIG["airflow"], "sla_minutes": 0}}
    yaml_str = yaml.dump({"pipeline": cfg})
    errors = CiValidator().validate_yaml(yaml_str)
    assert any("sla" in e.lower() for e in errors)


def test_dag_generator_guard_rejects_unknown_pipeline_type() -> None:
    import pytest
    import yaml

    from app.infrastructure.dag_generator.dag_generator import DagGenerator

    gen = DagGenerator()
    yaml_str = yaml.dump({"pipeline": {**VALID_CONFIG, "type": "invalid_type"}})
    with pytest.raises(ValueError, match="invalid_type"):
        gen.generate(yaml_str)
