from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable


class JobStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


@dataclass
class ComputeJobResult:
    """Result returned by monitor_compute_job."""

    job_id: str
    status: JobStatus
    metrics_path: str | None = None  # GCS/S3 path to metrics.json
    schema_path: str | None = None  # GCS/S3 path to schema.json
    output_path: str | None = None  # GCS/S3 path to parquet output
    error_message: str | None = None


@runtime_checkable
class ComputeJobAdapter(Protocol):
    """
    Protocol for compute job lifecycle management.

    Each compute engine (Spark, Dataflow, default) implements this.
    Ensures the same @task flow (submit -> monitor -> validate -> read_metrics)
    works across all pipeline types and compute backends.

    Example:
        adapter = SparkComputeJobAdapter(cluster_id="cluster-1")
        job_id = adapter.submit_job(pipeline_id, extraction_configs, staging_bucket)
        result = adapter.poll_job_status(job_id)
    """

    def submit_job(
        self,
        pipeline_id: str,
        pipeline_type: str,
        config: dict[str, Any],
    ) -> str:
        """Submit the compute job and return the job_id immediately (async submit)."""
        ...

    def poll_job_status(self, job_id: str) -> ComputeJobResult:
        """
        Poll the job status once. Called by @task.sensor in monitor_compute_job.
        Returns ComputeJobResult with current status. Sensor advances when status is terminal.
        """
        ...

    def cancel_job(self, job_id: str) -> None:
        """Cancel a running job. Called by on_failure_callback."""
        ...
