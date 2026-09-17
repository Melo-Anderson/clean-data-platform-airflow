from __future__ import annotations

from app.domain.pipelines.compute_engine import ComputeEngine
from app.infrastructure.dag_generator.ci_validator import CiValidator


def test_compute_engine_contains_dataform() -> None:
    assert ComputeEngine.DATAFORM == "dataform"
    assert "dataform" in [e.value for e in ComputeEngine]


def test_ci_validator_accepts_dataform_compute_engine() -> None:
    pipeline_yaml = """
pipeline:
  id: pipe-transform-dataform-001
  name: platform_dataform_transformation
  type: transformation
  owner: data@platform.internal
  schedule:
    mode: cron
    cron: "0 4 * * *"
  compute:
    engine: dataform
    staging_bucket: "gs://platform-dataform-staging"
"""
    validator = CiValidator()
    errors = validator.validate_yaml(pipeline_yaml)
    assert len(errors) == 0


def test_ci_validator_rejects_default_engine_for_transformation_pipeline() -> None:
    pipeline_yaml = """
pipeline:
  id: pipe-transform-invalid-001
  name: platform_invalid_transformation
  type: transformation
  owner: data@platform.internal
  compute:
    engine: default
"""
    validator = CiValidator()
    errors = validator.validate_yaml(pipeline_yaml)
    assert len(errors) > 0
    assert any("engine 'default' is not allowed for transformation" in err for err in errors)
