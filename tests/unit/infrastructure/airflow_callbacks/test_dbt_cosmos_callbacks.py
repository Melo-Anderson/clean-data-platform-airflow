from __future__ import annotations

import json
from pathlib import Path

from app.infrastructure.airflow_callbacks.dbt_cosmos_callbacks import collect_dbt_execution_metrics


def test_collect_dbt_execution_metrics_success(tmp_path: Path) -> None:
    project_dir = tmp_path / "dbt_proj"
    target_dir = project_dir / "target"
    target_dir.mkdir(parents=True)

    run_results = {
        "elapsed_time": 10.0,
        "results": [
            {"unique_id": "model.stg_a", "status": "success"},
            {"unique_id": "test.not_null_a", "status": "pass"},
        ],
    }
    (target_dir / "run_results.json").write_text(json.dumps(run_results), encoding="utf-8")

    out_dir = tmp_path / "outputs"

    res = collect_dbt_execution_metrics(
        pipeline_id="pipe-1",
        project_dir=project_dir,
        output_base_dir=out_dir,
    )

    assert res["status"] == "success"
    assert res["job_id"] == "dbt-cosmos-pipe-1"
    assert Path(res["metrics_path"]).exists()


def test_collect_dbt_execution_metrics_failed_tests(tmp_path: Path) -> None:
    project_dir = tmp_path / "dbt_proj"
    target_dir = project_dir / "target"
    target_dir.mkdir(parents=True)

    run_results = {
        "elapsed_time": 10.0,
        "results": [
            {"unique_id": "model.stg_a", "status": "success"},
            {"unique_id": "test.not_null_a", "status": "fail"},
        ],
    }
    (target_dir / "run_results.json").write_text(json.dumps(run_results), encoding="utf-8")

    out_dir = tmp_path / "outputs"

    res = collect_dbt_execution_metrics(
        pipeline_id="pipe-2",
        project_dir=project_dir,
        output_base_dir=out_dir,
    )

    assert res["status"] == "failed"
