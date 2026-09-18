from __future__ import annotations

import inspect
import json
import logging
import os
import shutil
import subprocess
import sys
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


def _default_dataform_executor(
    cmd: list[str], target_dir: Path, env_vars: dict[str, str] | None = None
) -> int:
    executable = shutil.which(cmd[0]) if cmd else None
    full_cmd = (
        [executable] + cmd[1:] if executable else [sys.executable, "-m", "dataform"] + cmd[1:]
    )

    env = dict(os.environ)
    if env_vars:
        env.update(env_vars)

    try:
        res = subprocess.run(full_cmd, capture_output=True, text=True, check=False, env=env)
        (target_dir / "dataform.log").write_text(
            f"STDOUT:\n{res.stdout}\n\nSTDERR:\n{res.stderr}", encoding="utf-8"
        )
        if res.returncode != 0:
            (target_dir / "error.txt").write_text(res.stderr or res.stdout, encoding="utf-8")
        return res.returncode
    except (OSError, subprocess.SubprocessError) as exc:
        (target_dir / "error.txt").write_text(str(exc), encoding="utf-8")
        return 1


class DataformComputeAdapter(ComputeJobAdapter):
    """Executes Dataform transformations and standardizes output to platform metrics."""

    def __init__(
        self,
        project_dir: str | Path,
        output_base_dir: str | Path,
        compilation_result_path: str | Path | None = None,
        executor_fn: Callable[..., int] | None = None,
        extra_env: dict[str, str] | None = None,
    ) -> None:
        self._project_dir = Path(project_dir)
        self._output_base_dir = Path(output_base_dir)
        self._compilation_result_path = (
            Path(compilation_result_path) if compilation_result_path else None
        )
        self._executor_fn = executor_fn or _default_dataform_executor
        self._extra_env = extra_env
        self._jobs: dict[str, dict[str, Any]] = {}

    def _execute_cmd(self, cmd: list[str], job_output_dir: Path) -> int:
        """Dispatch executor with optional env_vars if the signature supports it."""
        try:
            sig = inspect.signature(self._executor_fn)
            if len(sig.parameters) >= 3:
                return self._executor_fn(cmd, job_output_dir, self._extra_env)
        except (ValueError, TypeError) as exc:
            logger.debug(
                "Executor signature introspection failed: %s — falling back to 2-arg call", exc
            )
        return self._executor_fn(cmd, job_output_dir)

    def submit_job(
        self,
        pipeline_id: str,
        pipeline_type: str,
        config: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> str:
        job_id = f"dataform-job-{uuid.uuid4().hex[:12]}"
        job_output_dir = self._output_base_dir / pipeline_id / job_id
        job_output_dir.mkdir(parents=True, exist_ok=True)

        merged_config = {**(config or {}), **(params or {}), **kwargs}
        select_targets = merged_config.get("select", "")
        cmd = ["dataform", "run", "--project-dir", str(self._project_dir)]
        if select_targets:
            cmd.extend(["--actions", select_targets])

        exit_code = self._execute_cmd(cmd, job_output_dir)
        metrics_file, metrics_data = self._process_results(job_output_dir, exit_code)

        is_success = (exit_code == 0) and (metrics_data.get("tests_failed", 0) == 0)

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
        results_file = job_output_dir / "dataform_results.json"
        metrics_file = job_output_dir / "metrics.json"
        metrics_data = self._extract_metrics(results_file, exit_code)
        metrics_file.write_text(json.dumps(metrics_data, indent=2), encoding="utf-8")
        return metrics_file, metrics_data

    def _extract_metrics(self, results_path: Path, exit_code: int) -> dict[str, Any]:
        if not results_path.exists():
            return {
                "exit_code": exit_code,
                "models_passed": 0,
                "tests_passed": 0,
                "tests_failed": 1 if exit_code != 0 else 0,
                "elapsed_time": 0.0,
            }

        try:
            data = json.loads(results_path.read_text(encoding="utf-8"))
            actions = data.get("actions", [])
            models_passed = sum(
                1
                for a in actions
                if a.get("type") in ("table", "view", "incremental")
                and a.get("status") == "SUCCESS"
            )
            tests_passed = sum(
                1 for a in actions if a.get("type") == "assertion" and a.get("status") == "SUCCESS"
            )
            tests_failed = sum(
                1
                for a in actions
                if a.get("type") == "assertion" and a.get("status") in ("FAILED", "ERROR", "fail")
            )
            elapsed = float(data.get("elapsed_time", 0.0))
            return {
                "exit_code": exit_code,
                "models_passed": models_passed,
                "tests_passed": tests_passed,
                "tests_failed": tests_failed,
                "elapsed_time": elapsed,
            }
        except (json.JSONDecodeError, KeyError, ValueError) as exc:
            logger.error("Failed to parse Dataform results at %s: %s", results_path, exc)
            return {
                "exit_code": exit_code,
                "models_passed": 0,
                "tests_passed": 0,
                "tests_failed": 1,
                "elapsed_time": 0.0,
                "error": f"Malformed dataform_results.json: {exc}",
            }
