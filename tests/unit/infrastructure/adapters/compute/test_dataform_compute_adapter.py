from __future__ import annotations

import json
from pathlib import Path

from app.infrastructure.adapters.compute.dataform_compute_adapter import DataformComputeAdapter
from app.infrastructure.airflow_callbacks.compute_job_adapter import JobStatus


def test_dataform_compute_adapter_executes_and_generates_metrics(tmp_path: Path) -> None:
    output_dir = tmp_path / "dataform_outputs"
    output_dir.mkdir()

    def mock_dataform_executor(cmd: list[str], target_dir: Path) -> int:
        run_log = {
            "actions": [
                {"name": "slv_players", "type": "incremental", "status": "SUCCESS"},
                {"name": "dim_players", "type": "table", "status": "SUCCESS"},
                {"name": "assert_unique_player_sk", "type": "assertion", "status": "SUCCESS"},
            ],
            "elapsed_time": 4.2,
        }
        (target_dir / "dataform_results.json").write_text(json.dumps(run_log), encoding="utf-8")
        return 0

    adapter = DataformComputeAdapter(
        project_dir=str(tmp_path),
        output_base_dir=str(output_dir),
        executor_fn=mock_dataform_executor,
    )

    job_id = adapter.submit_job(
        pipeline_id="pipe-df-01",
        pipeline_type="transformation",
        params={"select": "dim_players"},
    )
    assert job_id.startswith("dataform-job-")

    status_result = adapter.poll_job_status(job_id)
    assert status_result.status == JobStatus.SUCCESS
    assert status_result.metrics_path is not None
    assert Path(status_result.metrics_path).exists()

    metrics = json.loads(Path(status_result.metrics_path).read_text(encoding="utf-8"))
    assert metrics["models_passed"] == 2
    assert metrics["tests_passed"] == 1
    assert metrics["tests_failed"] == 0
    assert metrics["elapsed_time"] == 4.2


def test_dataform_adapter_fails_when_exit_code_is_nonzero(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"

    def mock_failing_executor(cmd: list[str], target_dir: Path) -> int:
        (target_dir / "error.txt").write_text("Dataform compilation error", encoding="utf-8")
        return 1

    adapter = DataformComputeAdapter(
        project_dir=str(tmp_path),
        output_base_dir=str(out_dir),
        executor_fn=mock_failing_executor,
    )

    job_id = adapter.submit_job("pipe-df-fail", "transformation")
    result = adapter.poll_job_status(job_id)
    assert result.status == JobStatus.FAILED


def test_dataform_adapter_cancel_job() -> None:
    adapter = DataformComputeAdapter(
        project_dir="dataform_proj",
        output_base_dir="/tmp/outputs",
    )
    job_id = "test-df-cancel"
    adapter._jobs[job_id] = {"status": JobStatus.RUNNING}
    adapter.cancel_job(job_id)
    result = adapter.poll_job_status(job_id)
    assert result.status == JobStatus.CANCELLED


def test_get_transform_adapter_returns_dataform_adapter_for_dataform_engine() -> None:
    from app.infrastructure.compute_job_factory import get_transform_adapter

    adapter = get_transform_adapter("dataform")
    assert isinstance(adapter, DataformComputeAdapter)


def test_dataform_adapter_recovers_job_status_from_filesystem_across_instances(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "df_outputs"
    output_dir.mkdir()

    def mock_executor(cmd: list[str], target_dir: Path) -> int:
        (target_dir / "dataform_results.json").write_text(
            json.dumps({"results": [{"status": "success"}], "elapsed_time": 1.0}),
            encoding="utf-8",
        )
        return 0

    adapter1 = DataformComputeAdapter(
        project_dir=str(tmp_path),
        output_base_dir=str(output_dir),
        executor_fn=mock_executor,
    )
    job_id = adapter1.submit_job(pipeline_id="pipe-001", pipeline_type="transformation")

    adapter2 = DataformComputeAdapter(
        project_dir=str(tmp_path),
        output_base_dir=str(output_dir),
    )
    assert job_id not in adapter2._jobs

    result = adapter2.poll_job_status(job_id)
    assert result.status == JobStatus.SUCCESS
    assert result.metrics_path is not None
    assert Path(result.metrics_path).exists()
    assert result.output_path is not None
