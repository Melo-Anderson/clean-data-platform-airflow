from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.infrastructure.adapters.compute.omnibeam_compute_adapter import (
    OmniBeamComputeAdapter,
)
from app.infrastructure.airflow_callbacks.compute_job_adapter import (
    JobStatus,
)
from app.infrastructure.compute_job_factory import get_compute_adapter


@pytest.fixture
def mock_executor(tmp_path: Path):
    def fake_run_command(cmd: list[str], output_dir: Path) -> int:
        (output_dir / "metrics.json").write_text(
            json.dumps({"row_count": 150, "null_count": 0}), encoding="utf-8"
        )
        (output_dir / "data.parquet").write_bytes(b"PAR1fake")
        return 0

    return fake_run_command


def test_omnibeam_compute_adapter_submit_and_poll(tmp_path: Path, mock_executor) -> None:
    adapter = OmniBeamComputeAdapter(output_base_dir=str(tmp_path), executor_fn=mock_executor)
    config = {
        "manifest_json": json.dumps({"pipeline_id": "p1", "run_id": "r1"}),
    }

    job_id = adapter.submit_job(pipeline_id="p1", pipeline_type="ingestion", config=config)
    result = adapter.poll_job_status(job_id)

    assert result.status == JobStatus.SUCCESS
    assert result.job_id == job_id
    assert Path(result.output_path).exists()
    assert Path(result.metrics_path).exists()


def test_omnibeam_compute_adapter_failure(tmp_path: Path) -> None:
    def fake_failed_executor(cmd: list[str], output_dir: Path) -> int:
        (output_dir / "error.txt").write_text("Execution failed", encoding="utf-8")
        return 1

    adapter = OmniBeamComputeAdapter(
        output_base_dir=str(tmp_path), executor_fn=fake_failed_executor
    )
    config = {"manifest_json": "{}"}

    job_id = adapter.submit_job(pipeline_id="p2", pipeline_type="ingestion", config=config)
    result = adapter.poll_job_status(job_id)

    assert result.status == JobStatus.FAILED
    assert result.error_message == "Execution failed"


def test_compute_job_factory_returns_omnibeam_adapter() -> None:
    adapter = get_compute_adapter("omnibeam")
    assert isinstance(adapter, OmniBeamComputeAdapter)


def test_omnibeam_compute_adapter_direct_binary(tmp_path: Path) -> None:
    captured_commands: list[list[str]] = []

    def mock_binary_executor(cmd: list[str], output_dir: Path) -> int:
        captured_commands.append(cmd)
        (output_dir / "metrics.json").write_text(json.dumps({"row_count": 10}), encoding="utf-8")
        (output_dir / "data.parquet").write_bytes(b"PAR1")
        return 0

    fake_binary = tmp_path / "bin" / "omnibeam"
    fake_binary.parent.mkdir()
    fake_binary.write_text("#!/bin/sh\nexit 0", encoding="utf-8")

    adapter = OmniBeamComputeAdapter(
        output_base_dir=str(tmp_path / "outputs"),
        binary_path=str(fake_binary),
        executor_fn=mock_binary_executor,
    )
    job_id = adapter.submit_job("pipe-bin", "ingestion", {"manifest_json": "{}"})
    assert job_id is not None
    assert len(captured_commands) == 1
    assert captured_commands[0][0] == str(fake_binary)

    assert "--config_payload_path=" in captured_commands[0][1]
