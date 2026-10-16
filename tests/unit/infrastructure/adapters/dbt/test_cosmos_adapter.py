from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.infrastructure.adapters.dbt.cosmos.config_factory import build_cosmos_configs
from app.infrastructure.adapters.dbt.cosmos.run_results import (
    RunResultsCollector,
)
from app.infrastructure.adapters.dbt.cosmos.settings import CosmosDbtSettings


def test_cosmos_settings_fail_fast_on_missing_project(tmp_path: Path) -> None:
    settings = CosmosDbtSettings(
        project_dir=tmp_path / "non_existing_project",
        profiles_dir=tmp_path,
        manifest_path=tmp_path / "manifest.json",
    )
    with pytest.raises(FileNotFoundError, match="project_dir does not exist"):
        settings.validate()


def test_cosmos_settings_fail_fast_on_missing_manifest(tmp_path: Path) -> None:
    project_dir = tmp_path / "dbt_proj"
    project_dir.mkdir()
    settings = CosmosDbtSettings(
        project_dir=project_dir,
        profiles_dir=project_dir,
        manifest_path=project_dir / "target" / "manifest.json",
    )
    with pytest.raises(FileNotFoundError, match="manifest.json does not exist"):
        settings.validate()


def test_build_cosmos_configs_extracts_clean_select(tmp_path: Path) -> None:
    project_dir = tmp_path / "dbt_proj"
    project_dir.mkdir()
    manifest_file = project_dir / "manifest.json"
    manifest_file.write_text("{}", encoding="utf-8")
    profiles_file = project_dir / "profiles.yml"
    profiles_file.write_text("{}", encoding="utf-8")

    settings = CosmosDbtSettings(
        project_dir=project_dir,
        profiles_dir=project_dir,
        manifest_path=manifest_file,
        virtualenv_dir=tmp_path / "venv",
    )
    configs = build_cosmos_configs(settings, {"select": "tag:silver model_orders"})
    assert configs["select"] == ["tag:silver", "model_orders"]
    assert configs["project_dir"] == str(project_dir)
    assert configs["manifest_path"] == str(manifest_file)
    assert configs["virtualenv_dir"] == str(tmp_path / "venv")


def test_run_results_collector_extracts_metrics(tmp_path: Path) -> None:
    run_results = {
        "elapsed_time": 42.5,
        "results": [
            {"unique_id": "model.stg_orders", "status": "success"},
            {"unique_id": "model.fct_orders", "status": "success"},
            {"unique_id": "test.not_null", "status": "pass"},
            {"unique_id": "test.unique", "status": "fail"},
        ],
    }
    results_path = tmp_path / "run_results.json"
    results_path.write_text(json.dumps(run_results), encoding="utf-8")
    target_metrics = tmp_path / "metrics.json"

    collector = RunResultsCollector()
    metrics = collector.collect(results_path, target_metrics)

    assert metrics.models_passed == 2
    assert metrics.tests_passed == 1
    assert metrics.tests_failed == 1
    assert metrics.exit_code == 1
    assert metrics.elapsed_time == 42.5

    saved = json.loads(target_metrics.read_text(encoding="utf-8"))
    assert saved["tests_failed"] == 1
    assert saved["models_passed"] == 2


def test_run_results_collector_fails_fast_on_missing_file(tmp_path: Path) -> None:
    collector = RunResultsCollector()
    with pytest.raises(FileNotFoundError):
        collector.collect(tmp_path / "non_existing.json")


def test_run_results_collector_fails_fast_on_invalid_schema(tmp_path: Path) -> None:
    bad_file = tmp_path / "bad.json"
    bad_file.write_text(json.dumps({"invalid": "schema"}), encoding="utf-8")
    collector = RunResultsCollector()
    with pytest.raises(ValueError, match="missing 'results' field"):
        collector.collect(bad_file)
