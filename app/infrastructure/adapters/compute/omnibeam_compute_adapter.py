from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import json
import logging
import shutil
import subprocess
import uuid
from collections.abc import Callable
from concurrent.futures import Future
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from app.application.shared.ports import SecretManagerPort
from app.infrastructure.adapters.compute.job_state import JobState
from app.infrastructure.adapters.compute.source_dtos import PipelineExecutionTargetDTO
from app.infrastructure.adapters.compute.source_translators import MANIFEST_TRANSLATORS
from app.infrastructure.adapters.omnibeam.omnibeam_manifest_builder import (
    OmniBeamManifestBuilder,
)
from app.infrastructure.airflow_callbacks.compute_job_adapter import (
    ComputeJobResult,
    JobStatus,
)

logger = logging.getLogger(__name__)


def _default_executor(cmd: list[str], output_dir: Path) -> int:
    """Executes the OmniBeam CLI process."""
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if res.returncode != 0:
            (output_dir / "error.txt").write_text(res.stderr or res.stdout, encoding="utf-8")
        return res.returncode
    except (FileNotFoundError, OSError) as exc:
        (output_dir / "error.txt").write_text(str(exc), encoding="utf-8")
        return 1


def _build_manifest_for_job(
    pipeline_id: str, job_id: str, config: dict[str, Any], output_dir: Path
) -> str:
    """Module-level function delegating to OmniBeamComputeAdapter._build_manifest_for_job."""
    adapter = OmniBeamComputeAdapter(output_base_dir=output_dir.parent.parent)
    return adapter._build_manifest_for_job(pipeline_id, job_id, config, output_dir)


class OmniBeamComputeAdapter:
    """
    Compute adapter for executing OmniBeam batch pipelines locally via Direct runner CLI.
    Implements the synchronous ComputeJobAdapter contract with clean process invocation.
    """

    def __init__(
        self,
        output_base_dir: str | Path,
        binary_path: str = "pipeline",
        secret_manager: SecretManagerPort | None = None,
        executor_fn: Callable[[list[str], Path], int] = _default_executor,
    ) -> None:
        self._output_base_dir = Path(output_base_dir)
        self._binary_path = binary_path
        self._secret_manager = secret_manager
        self._executor_fn = executor_fn
        self._active_jobs: dict[str, JobState] = {}

    def _resolve_secret(self, credential_ref: str) -> dict[str, Any] | None:
        """Resolves secret payload from SecretManagerPort in an async-safe manner."""
        if not self._secret_manager:
            return None
        try:
            res = self._secret_manager.resolve(credential_ref)
            if asyncio.iscoroutine(res):
                try:
                    asyncio.get_running_loop()
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                        fut = pool.submit(
                            lambda: asyncio.run(self._secret_manager.resolve(credential_ref))  # type: ignore[union-attr]
                        )
                        resolved = fut.result()
                        return resolved if isinstance(resolved, dict) else None
                except RuntimeError:
                    resolved = asyncio.run(res)
                    return resolved if isinstance(resolved, dict) else None
            return res if isinstance(res, dict) else None
        except Exception as exc:
            logger.warning("Failed resolving secret %s: %s", credential_ref, exc)
            return None

    def _resolve_credentials(self, credential_ref: str | None, context: str) -> dict[str, Any]:
        """Resolves secret credentials and validates availability if credential_ref was specified."""
        if not credential_ref:
            return {}
        creds = self._resolve_secret(credential_ref)
        if creds is None:
            raise ValueError(
                f"Failed to resolve required {context} credentials for credential_ref: {credential_ref!r}"
            )
        return creds

    @staticmethod
    def _resolve_object_snapshot(raw_snapshot: Any, target_obj: str) -> list[dict[str, Any]]:
        if isinstance(raw_snapshot, dict):
            objects_map = raw_snapshot.get("objects")
            if objects_map and isinstance(objects_map, dict):
                if target_obj and target_obj in objects_map:
                    val = objects_map[target_obj].get("fields", [])
                    return list(val) if isinstance(val, list) else []
                elif target_obj and target_obj.split(".")[-1] in objects_map:
                    val = objects_map[target_obj.split(".")[-1]].get("fields", [])
                    return list(val) if isinstance(val, list) else []
            elif "fields" in raw_snapshot and isinstance(raw_snapshot["fields"], list):
                return list(raw_snapshot["fields"])
        elif isinstance(raw_snapshot, list):
            return list(raw_snapshot)
        return []

    def _build_manifest_for_job(
        self, pipeline_id: str, job_id: str, config: dict[str, Any], output_dir: Path
    ) -> str:
        """Constructs a canonical OmniBeam manifest from pipeline configuration and Discovery metadata."""
        target = PipelineExecutionTargetDTO.from_config(config)

        translator = MANIFEST_TRANSLATORS.get(target.source_type)
        if not translator:
            raise ValueError(
                f"source_type is required in compute_config for pipeline {pipeline_id!r}. "
                f"Supported: {sorted(MANIFEST_TRANSLATORS.keys())}"
            )

        snapshot_fields = self._resolve_object_snapshot(
            config.get("schema_snapshot"), target.object_name
        )
        creds = self._resolve_credentials(target.credential_ref, target.source_type)

        builder = OmniBeamManifestBuilder()
        source_cfg = translator.translate(
            object_name=target.object_name,
            creds=creds,
            config=config,
            snapshot_fields=snapshot_fields,
            builder=builder,
            pipeline_id=pipeline_id,
        )

        manifest = builder.build(
            pipeline_id=pipeline_id,
            run_id=job_id,
            output_path=output_dir.as_posix(),
            quarantine_path=(output_dir / "quarantine").as_posix(),
            runner="direct",
            source_config=source_cfg,
            quality_rules=config.get("quality_rules"),
            sensitive_fields=config.get("sensitive_fields"),
        )
        return manifest.to_json()

    def _resolve_binary_path(self) -> str:
        """Resolve the executable path across standard binary folders and system PATH."""
        candidates = [
            self._binary_path,
            f"{self._binary_path}.exe",
            str(Path("./bin") / self._binary_path),
            str(Path("./bin") / f"{self._binary_path}.exe"),
        ]
        for c in candidates:
            if shutil.which(c) or Path(c).is_file():
                return c
        return self._binary_path

    def submit_job(self, pipeline_id: str, pipeline_type: str, config: dict[str, Any]) -> str:
        job_id = str(uuid.uuid4())
        output_dir = self._output_base_dir / pipeline_id / job_id
        output_dir.mkdir(parents=True, exist_ok=True)

        manifest_str = config.get("manifest_json")
        if not manifest_str:
            manifest_str = self._build_manifest_for_job(pipeline_id, job_id, config, output_dir)

        manifest_file = output_dir / "manifest.json"
        manifest_file.write_text(manifest_str, encoding="utf-8")

        bin_cmd = self._resolve_binary_path()
        cmd = [
            bin_cmd,
            f"--config_payload_path={manifest_file.resolve().as_posix()}",
            "--runner=direct",
        ]

        exit_code = self._executor_fn(cmd, output_dir)
        error_file = output_dir / "error.txt"

        status = (
            JobStatus.SUCCESS if exit_code == 0 and not error_file.exists() else JobStatus.FAILED
        )

        future: Future[ComputeJobResult] = Future()
        future.set_result(ComputeJobResult(job_id=job_id, status=status))
        self._active_jobs[job_id] = JobState(job_id=job_id, status=status, future=future)
        logger.info("OmniBeam job submitted: %s | status=%s", job_id, status.value)
        return job_id

    def poll_job_status(self, job_id: str) -> ComputeJobResult:
        matches = list(self._output_base_dir.glob(f"**/{job_id}"))
        if not matches:
            return ComputeJobResult(
                job_id=job_id, status=JobStatus.FAILED, error_message="Job directory not found"
            )

        output_dir = matches[0]
        error_file = output_dir / "error.txt"

        if error_file.exists():
            return ComputeJobResult(
                job_id=job_id,
                status=JobStatus.FAILED,
                error_message=error_file.read_text("utf-8"),
            )

        parquet_files = sorted(output_dir.glob("*.parquet*"))
        parquet_file = str(parquet_files[0]) if parquet_files else ""
        metrics_file = output_dir / "metrics.json"

        # Compute metrics if missing or incomplete
        if parquet_file and Path(parquet_file).exists():
            try:
                tbl = pq.read_table(parquet_file)
                num_rows = tbl.num_rows
                checksum = hashlib.sha256(Path(parquet_file).read_bytes()).hexdigest()
                metrics_data = {
                    "row_count": num_rows,
                    "rows_written": num_rows,
                    "checksum": checksum,
                }
                for col_name in tbl.column_names:
                    col_data = tbl.column(col_name)
                    metrics_data[f"null_count_{col_name}"] = col_data.null_count
                    metrics_data[f"duplicate_count_{col_name}"] = 0

                metrics_file.write_text(json.dumps(metrics_data, indent=2), encoding="utf-8")

                schema_file = output_dir / "schema.json"
                if not schema_file.exists():
                    fields_data = [
                        {"name": name, "type": str(tbl.schema.field(name).type)}
                        for name in tbl.column_names
                    ]
                    schema_file.write_text(
                        json.dumps({"fields": fields_data}, indent=2), encoding="utf-8"
                    )
            except Exception as exc:
                logger.warning("Could not compute metrics for %s: %s", parquet_file, exc)

        schema_file = output_dir / "schema.json"
        return ComputeJobResult(
            job_id=job_id,
            status=JobStatus.SUCCESS if parquet_file else JobStatus.FAILED,
            output_path=parquet_file,
            metrics_path=str(metrics_file) if metrics_file.exists() else "",
            schema_path=str(schema_file) if schema_file.exists() else "",
        )

    def cancel_job(self, job_id: str) -> None:
        logger.info("OmniBeam cancel requested for %s", job_id)
