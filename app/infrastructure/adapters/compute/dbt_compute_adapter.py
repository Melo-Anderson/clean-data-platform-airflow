from __future__ import annotations

import json
import logging
import subprocess
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from app.infrastructure.airflow_callbacks.compute_job_adapter import (
    ComputeJobAdapter,
    ComputeJobResult,
    JobStatus,
)

logger = logging.getLogger(__name__)


def _default_dbt_executor(cmd: list[str], target_dir: Path) -> int:
    import os
    import shutil
    import sys

    executable = shutil.which(cmd[0]) if cmd else None
    full_cmd = [executable] + cmd[1:] if executable else [sys.executable, "-m", "dbt"] + cmd[1:]

    env = dict(os.environ)
    gcp_creds = env.get("GOOGLE_APPLICATION_CREDENTIALS")
    gcp_host = env.get("GOOGLE_APPLICATION_CREDENTIALS_HOST")

    if gcp_creds and not Path(gcp_creds).exists():
        if gcp_host and Path(gcp_host).exists():
            env["GOOGLE_APPLICATION_CREDENTIALS"] = Path(gcp_host).resolve().as_posix()
        else:
            env.pop("GOOGLE_APPLICATION_CREDENTIALS", None)

    try:
        res = subprocess.run(full_cmd, capture_output=True, text=True, check=False, env=env)
        (target_dir / "dbt.log").write_text(
            f"STDOUT:\n{res.stdout}\n\nSTDERR:\n{res.stderr}", encoding="utf-8"
        )
        if res.returncode != 0:
            (target_dir / "error.txt").write_text(res.stderr or res.stdout, encoding="utf-8")
        return res.returncode
    except Exception as exc:
        (target_dir / "error.txt").write_text(str(exc), encoding="utf-8")
        return 1


class DbtComputeAdapter(ComputeJobAdapter):
    """Executes dbt transformations (dbt build/run/test) and converts run results to platform metrics."""

    def __init__(
        self,
        project_dir: str = "dbt_project",
        profiles_dir: str = "dbt_project",
        output_base_dir: str = "/opt/airflow/logs/dbt_outputs",
        executor_fn: Callable[[list[str], Path], int] | None = None,
    ) -> None:
        self._project_dir = Path(project_dir)
        self._profiles_dir = Path(profiles_dir)
        self._output_base_dir = Path(output_base_dir)
        self._executor_fn = executor_fn or _default_dbt_executor
        self._jobs: dict[str, dict[str, Any]] = {}

    def submit_job(
        self,
        pipeline_id: str,
        pipeline_type: str,
        config: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> str:
        job_id = f"dbt-job-{uuid.uuid4().hex[:12]}"
        job_output_dir = self._output_base_dir / pipeline_id / job_id
        job_output_dir.mkdir(parents=True, exist_ok=True)

        merged_config = {**(config or {}), **(params or {}), **kwargs}
        select_models = merged_config.get("select", "")
        cmd = [
            "dbt",
            "build",
            "--project-dir",
            str(self._project_dir),
            "--profiles-dir",
            str(self._profiles_dir),
        ]
        if select_models:
            cmd.extend(["--select", select_models])

        exit_code = self._executor_fn(cmd, job_output_dir)
        metrics_file, metrics_data = self._process_results(job_output_dir, exit_code)

        is_success = exit_code == 0 or (
            metrics_data.get("tests_failed", 0) == 0 and metrics_data.get("models_passed", 0) > 0
        )

        self._jobs[job_id] = {
            "status": JobStatus.SUCCESS if is_success else JobStatus.FAILED,
            "metrics_path": str(metrics_file),
            "output_path": str(job_output_dir),
        }
        return job_id

    def poll_job_status(self, job_id: str) -> ComputeJobResult:
        if job_id not in self._jobs:
            return ComputeJobResult(
                job_id=job_id, status=JobStatus.FAILED, error_message="Job not found"
            )

        info = self._jobs[job_id]
        return ComputeJobResult(
            job_id=job_id,
            status=info["status"],
            metrics_path=info.get("metrics_path"),
            output_path=info.get("output_path"),
        )

    def cancel_job(self, job_id: str) -> None:
        if job_id in self._jobs:
            self._jobs[job_id]["status"] = JobStatus.CANCELLED

    def _process_results(self, job_output_dir: Path, exit_code: int) -> tuple[Path, dict[str, Any]]:
        run_results_file = job_output_dir / "run_results.json"
        if not run_results_file.exists():
            # Fallback to project target dir if copied
            run_results_file = self._project_dir / "target" / "run_results.json"

        metrics_file = job_output_dir / "metrics.json"
        metrics_data = self._extract_metrics(run_results_file, exit_code)
        metrics_file.write_text(json.dumps(metrics_data, indent=2), encoding="utf-8")
        return metrics_file, metrics_data

    def _extract_metrics(self, run_results_path: Path, exit_code: int) -> dict[str, Any]:
        if not run_results_path.exists():
            return {
                "exit_code": exit_code,
                "models_passed": 0,
                "tests_passed": 0,
                "tests_failed": 1 if exit_code != 0 else 0,
                "elapsed_time": 0.0,
            }

        try:
            data = json.loads(run_results_path.read_text(encoding="utf-8"))
            results = data.get("results", [])
            models_passed = sum(1 for r in results if r.get("status") == "success")
            tests_passed = sum(1 for r in results if r.get("status") == "pass")
            tests_failed = sum(1 for r in results if r.get("status") in ("fail", "error"))
            elapsed = float(data.get("elapsed_time", 0.0))
            return {
                "exit_code": exit_code,
                "models_passed": models_passed,
                "tests_passed": tests_passed,
                "tests_failed": tests_failed,
                "elapsed_time": elapsed,
            }
        except Exception:
            return {"exit_code": exit_code, "error": "Malformed run_results.json"}
