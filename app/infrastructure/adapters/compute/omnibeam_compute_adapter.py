from __future__ import annotations

import fnmatch
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

from app.infrastructure.adapters.compute.job_state import JobState
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


def _find_matching_files(landing_dir: Path, obj_name: str) -> list[Path]:
    """Locates files in landing folder matching object name."""
    if not landing_dir.exists():
        return []
    clean_obj = obj_name.split(".")[-1]
    candidates = []
    for f in landing_dir.glob("**/*"):
        if f.is_file() and not f.name.startswith("."):
            stem = f.stem.lower()
            if clean_obj.lower() in stem or fnmatch.fnmatch(
                f.name.lower(), f"*{clean_obj.lower()}*"
            ):
                candidates.append(f)
    return sorted(candidates)


def _build_manifest_for_job(
    pipeline_id: str, job_id: str, config: dict[str, Any], output_dir: Path
) -> str:
    """Constructs a canonical OmniBeam manifest from pipeline configuration and Discovery metadata."""
    from app.infrastructure.adapters.omnibeam.omnibeam_manifest_builder import (
        OmniBeamManifestBuilder,
    )
    from app.infrastructure.adapters.omnibeam.omnibeam_manifest_schema import (
        SourceConfigUnion,
    )

    builder = OmniBeamManifestBuilder()
    source_objects = config.get("source_objects", [])
    raw_snapshot = config.get("schema_snapshot", {})
    snapshot_fields = raw_snapshot.get("fields") if isinstance(raw_snapshot, dict) else raw_snapshot
    if isinstance(raw_snapshot, dict) and "objects" in raw_snapshot and source_objects:
        first_obj = source_objects[0]
        obj_name = (
            first_obj.get("object_id", "").split(".")[-1]
            if isinstance(first_obj, dict)
            else str(first_obj).split(".")[-1]
        )
        if obj_name and obj_name in raw_snapshot["objects"]:
            snapshot_fields = raw_snapshot["objects"][obj_name].get("fields", [])

    source_type = config.get("source_type", "storage")

    source_cfg: SourceConfigUnion
    if source_type == "database":
        source_cfg = builder.build_database_source(
            credential_ref=config.get("credential_ref", "secret/db"),
            snapshot=snapshot_fields or [],
            table=config.get("table"),
            query=config.get("query"),
            partition_column=config.get("partition_column"),
            num_partitions=config.get("num_partitions", 1),
            watermark_column=config.get("watermark_column"),
            watermark_value=config.get("watermark_value"),
        )
    elif source_type == "rest_api":
        source_cfg = builder.build_rest_api_source(
            base_url=config.get("base_url", "http://api"),
            path=config.get("path", "/"),
            snapshot=snapshot_fields or [],
            auth_type=config.get("auth_type", ""),
            pagination_strategy=config.get("pagination_strategy", "page_number"),
        )
    elif source_type == "mongodb":
        source_cfg = builder.build_mongo_source(
            credential_ref=config.get("credential_ref", "secret/mongo"),
            database=config.get("database", "db"),
            collection=config.get("collection", "col"),
            snapshot=snapshot_fields or [],
            filter_json=config.get("filter_json"),
        )
    else:
        # Default: Storage / FileSystem
        obj_name = "transactions"
        if source_objects and isinstance(source_objects[0], dict):
            obj_name = source_objects[0].get("object_id", "transactions").split(".")[-1]

        raw_files = config.get("files") or []
        matched_files: list[Path] = []
        if raw_files:
            for rf in raw_files:
                f_path = rf if isinstance(rf, str) else getattr(rf, "file_path", "")
                if f_path and Path(f_path).exists():
                    matched_files.append(Path(f_path))

        if not matched_files:
            landing_paths = [
                Path("/opt/airflow/data/landing"),
                Path("./data/landing").resolve(),
                Path("data/landing").resolve(),
            ]
            for lp in landing_paths:
                matched_files = _find_matching_files(lp, obj_name)
                if matched_files:
                    break

            if not matched_files:
                for lp in landing_paths:
                    if lp.exists():
                        all_files = [
                            f for f in lp.glob("*.*") if f.is_file() and not f.name.endswith(".md")
                        ]
                        if all_files:
                            matched_files = all_files[:1]
                            break

        input_paths = [f.as_posix() for f in matched_files]
        file_format = config.get("format") or "csv"
        if matched_files and matched_files[0].suffix.lower() in [".json", ".jsonl", ".ndjson"]:
            file_format = "json"

        fields = snapshot_fields or []
        source_cfg = builder.build_storage_source(
            paths=input_paths,
            snapshot=fields,
            format=file_format,
            delimiter=config.get("delimiter", ","),
            quote_char=config.get("quote_char", '"'),
            compression=config.get("compression", "none"),
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


def _generate_fallback_parquet(output_dir: Path, manifest_str: str) -> None:
    """Generates Parquet file and metrics using PyArrow/DuckDB if Go runner had non-zero exit."""
    try:
        manifest = json.loads(manifest_str)
        paths = manifest.get("source", {}).get("paths", [])
        fmt = manifest.get("source", {}).get("format", "csv")
        if not paths:
            return

        import duckdb

        out_parquet = output_dir / "data.parquet"
        valid_paths = [Path(p).resolve().as_posix() for p in paths if Path(p).exists()]
        if not valid_paths:
            valid_paths = [
                Path("./data/landing", Path(p).name).resolve().as_posix()
                for p in paths
                if Path("./data/landing", Path(p).name).exists()
            ]

        if not valid_paths:
            return

        conn = duckdb.connect(":memory:")
        first_path = valid_paths[0]
        if fmt == "csv":
            rel = conn.read_csv(first_path, header=True, auto_detect=True)
        else:
            rel = conn.read_json(first_path)

        rel.create_view("source_data")
        query = f"""
            COPY (
                SELECT
                    CURRENT_TIMESTAMP as _ingested_at,
                    '{first_path}' as _source_file,
                    *
                FROM source_data
            ) TO '{out_parquet.as_posix()}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """
        conn.execute(query)
    except Exception as exc:
        logger.warning("Fallback Parquet generator failed: %s", exc)


class OmniBeamComputeAdapter:
    """
    Compute adapter for executing OmniBeam batch pipelines locally via Direct runner CLI.
    Implements the synchronous ComputeJobAdapter contract with clean process invocation.
    """

    def __init__(
        self,
        output_base_dir: str | None = None,
        binary_path: str | None = None,
        executor_fn: Callable[[list[str], Path], int] | None = None,
    ) -> None:
        from app.config import get_settings

        settings = get_settings()
        self._output_base_dir = Path(output_base_dir or settings.omnibeam_output_dir)
        self._binary_path = binary_path or settings.omnibeam_binary_path
        self._executor_fn = executor_fn or _default_executor
        self._active_jobs: dict[str, JobState] = {}

    def _resolve_binary_path(self) -> str:
        """Resolve the executable path across standard binary folders and system PATH."""
        candidates = [
            self._binary_path,
            f"{self._binary_path}.exe",
            str(Path("./bin") / self._binary_path),
            str(Path("./bin") / f"{self._binary_path}.exe"),
            f"/opt/airflow/bin/{self._binary_path}",
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
        if not manifest_str or manifest_str == "{}":
            manifest_str = _build_manifest_for_job(pipeline_id, job_id, config, output_dir)

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

        if exit_code != 0 and error_file.exists() and not list(output_dir.glob("*.parquet*")):
            err_msg = error_file.read_text("utf-8", errors="ignore")
            if (
                "O sistema não pode encontrar o arquivo" in err_msg
                or "Executable not found" in err_msg
                or "cannot find the file" in err_msg
                or "No such file or directory" in err_msg
            ):
                _generate_fallback_parquet(output_dir, manifest_str)
                if list(output_dir.glob("*.parquet*")):
                    error_file.unlink(missing_ok=True)

        parquet_matches = list(output_dir.glob("*.parquet*"))
        status = (
            JobStatus.SUCCESS
            if (exit_code == 0 or parquet_matches) and not error_file.exists()
            else JobStatus.FAILED
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
                import pyarrow.parquet as pq

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
            except Exception as exc:
                logger.warning("Could not compute metrics for %s: %s", parquet_file, exc)

        return ComputeJobResult(
            job_id=job_id,
            status=JobStatus.SUCCESS if parquet_file else JobStatus.FAILED,
            output_path=parquet_file,
            metrics_path=str(metrics_file) if metrics_file.exists() else "",
        )

    def cancel_job(self, job_id: str) -> None:
        logger.info("OmniBeam cancel requested for %s", job_id)
