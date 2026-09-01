from __future__ import annotations

import json
from pathlib import Path

from app.infrastructure.adapters.compute.dbt_compute_adapter import DbtComputeAdapter
from app.infrastructure.airflow_callbacks.compute_job_adapter import JobStatus
from app.infrastructure.compute_job_factory import get_transform_adapter


def test_dbt_compute_adapter_executes_and_generates_metrics(tmp_path: Path) -> None:
    output_dir = tmp_path / "dbt_outputs"
    output_dir.mkdir()

    def mock_dbt_executor(cmd: list[str], target_dir: Path) -> int:
        run_results = {
            "results": [
                {
                    "unique_id": "model.platform.stg_players",
                    "status": "success",
                    "execution_time": 1.2,
                },
                {
                    "unique_id": "model.platform.slv_players",
                    "status": "success",
                    "execution_time": 2.5,
                },
                {
                    "unique_id": "test.platform.unique_slv_players",
                    "status": "pass",
                    "execution_time": 0.8,
                },
                {
                    "unique_id": "test.platform.assert_positive_deposits",
                    "status": "pass",
                    "execution_time": 0.5,
                },
            ],
            "elapsed_time": 5.0,
        }
        (target_dir / "run_results.json").write_text(json.dumps(run_results), encoding="utf-8")
        return 0

    adapter = DbtComputeAdapter(
        project_dir=str(tmp_path),
        output_base_dir=str(output_dir),
        executor_fn=mock_dbt_executor,
    )

    job_id = adapter.submit_job(
        pipeline_id="pipe-dbt-01",
        pipeline_type="transformation",
        params={"select": "models/silver"},
    )
    assert job_id.startswith("dbt-job-")

    status_result = adapter.poll_job_status(job_id)
    assert status_result.status == JobStatus.SUCCESS
    assert status_result.metrics_path is not None
    assert Path(status_result.metrics_path).exists()

    metrics = json.loads(Path(status_result.metrics_path).read_text(encoding="utf-8"))
    assert metrics["models_passed"] == 2
    assert metrics["tests_passed"] == 2
    assert metrics["tests_failed"] == 0
    assert metrics["elapsed_time"] == 5.0


def test_cancel_job_sets_cancelled_status() -> None:
    adapter = DbtComputeAdapter(
        project_dir="dbt_project",
        profiles_dir="dbt_project",
        output_base_dir="/tmp/dbt_outputs",
    )
    job_id = "test-job-cancel"
    adapter._jobs[job_id] = {"status": JobStatus.RUNNING}
    adapter.cancel_job(job_id)
    result = adapter.poll_job_status(job_id)
    assert result.status == JobStatus.CANCELLED


def test_poll_unknown_job_returns_failed() -> None:
    adapter = DbtComputeAdapter(
        project_dir="dbt_project",
        profiles_dir="dbt_project",
        output_base_dir="/tmp/dbt_outputs",
    )
    result = adapter.poll_job_status("non-existent-job-id")
    assert result.status == JobStatus.FAILED


def test_get_transform_adapter_returns_dbt_adapter_for_dbt_engine() -> None:
    adapter = get_transform_adapter("dbt")
    assert isinstance(adapter, DbtComputeAdapter)


def test_dbt_adapter_fails_when_exit_code_is_nonzero(tmp_path: Path) -> None:
    target_dir = tmp_path / "dbt_project"
    target_dir.mkdir()
    out_dir = tmp_path / "outputs"

    def mock_executor(cmd: list[str], output_dir: Path, env: dict[str, str] | None = None) -> int:
        (output_dir / "error.txt").write_text("Compilation Error in model x", encoding="utf-8")
        return 2

    adapter = DbtComputeAdapter(
        project_dir=target_dir,
        profiles_dir=target_dir,
        output_base_dir=out_dir,
        executor_fn=mock_executor,
    )

    job_id = adapter.submit_job("test-pipe", "transformation")
    result = adapter.poll_job_status(job_id)

    assert result.status == JobStatus.FAILED


def test_dbt_adapter_does_not_fallback_to_stale_project_target(tmp_path: Path) -> None:
    target_dir = tmp_path / "dbt_project"
    target_dir.mkdir()
    (target_dir / "target").mkdir()
    (target_dir / "target" / "run_results.json").write_text(
        '{"results": [{"status": "success"}]}', encoding="utf-8"
    )
    out_dir = tmp_path / "outputs"

    def failing_executor(
        cmd: list[str], output_dir: Path, env: dict[str, str] | None = None
    ) -> int:
        (output_dir / "error.txt").write_text("DB connection timeout", encoding="utf-8")
        return 1

    adapter = DbtComputeAdapter(
        project_dir=target_dir,
        profiles_dir=target_dir,
        output_base_dir=out_dir,
        executor_fn=failing_executor,
    )

    job_id = adapter.submit_job("test-pipe", "transformation")
    result = adapter.poll_job_status(job_id)

    assert result.status == JobStatus.FAILED
