from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.infrastructure.airflow_callbacks import transformation_callbacks


def test_legacy_dbt_wrapper_exists() -> None:
    assert callable(transformation_callbacks.run_dbt_transformation_job)
    assert callable(transformation_callbacks.evaluate_dbt_quality_gates)
    assert callable(transformation_callbacks.sync_dbt_catalog_metadata)


def test_evaluate_transformation_quality_gates_passes_when_zero_failures(tmp_path: Path) -> None:
    metrics_file = tmp_path / "metrics.json"
    metrics_file.write_text(json.dumps({"tests_passed": 5, "tests_failed": 0}), encoding="utf-8")

    result = transformation_callbacks.evaluate_transformation_quality_gates(
        pipeline_id="pipe-1",
        metrics_path=str(metrics_file),
    )
    assert result["quality_ok"] is True
    assert result["tests_passed"] == 5
    assert result["tests_failed"] == 0


def test_evaluate_transformation_quality_gates_raises_keyerror_on_malformed_metrics(
    tmp_path: Path,
) -> None:
    metrics_file = tmp_path / "metrics.json"
    metrics_file.write_text(json.dumps({"unrelated_metric": 42}), encoding="utf-8")

    with pytest.raises(KeyError) as exc_info:
        transformation_callbacks.evaluate_transformation_quality_gates(
            pipeline_id="pipe-1",
            metrics_path=str(metrics_file),
        )
    assert "tests_failed" in str(exc_info.value) or "tests_passed" in str(exc_info.value)


def test_evaluate_transformation_quality_gates_raises_runtime_error_on_failures(
    tmp_path: Path,
) -> None:
    metrics_file = tmp_path / "metrics.json"
    metrics_file.write_text(json.dumps({"tests_passed": 2, "tests_failed": 3}), encoding="utf-8")

    with pytest.raises(RuntimeError) as exc_info:
        transformation_callbacks.evaluate_transformation_quality_gates(
            pipeline_id="pipe-1",
            metrics_path=str(metrics_file),
        )
    assert "Quality Gate Failed for transformation pipeline 'pipe-1'" in str(exc_info.value)
    assert "3 transformation quality tests failed" in str(exc_info.value)


def test_sync_transformation_catalog_metadata_raises_for_unsupported_engine() -> None:
    with pytest.raises(ValueError) as exc_info:
        transformation_callbacks.sync_transformation_catalog_metadata(
            asset_id="asset-1",
            manifest_path="/path/manifest.json",
            engine="dataform",
        )
    assert "dataform" in str(exc_info.value)


def test_sync_transformation_catalog_metadata_raises_filenotfound_when_manifest_missing(
    tmp_path: Path,
) -> None:
    missing_manifest = tmp_path / "nonexistent.json"
    with pytest.raises(FileNotFoundError) as exc_info:
        transformation_callbacks.sync_transformation_catalog_metadata(
            asset_id="asset-1",
            manifest_path=str(missing_manifest),
            engine="dbt",
        )
    assert "Manifest file not found" in str(exc_info.value)


def test_run_transformation_job_raises_value_error_when_output_base_dir_empty() -> None:
    with pytest.raises(ValueError) as exc_info:
        transformation_callbacks.run_transformation_job(
            pipeline_id="pipe-1",
            engine="dbt",
            output_base_dir="",
        )
    assert "output_base_dir must not be empty" in str(exc_info.value)
