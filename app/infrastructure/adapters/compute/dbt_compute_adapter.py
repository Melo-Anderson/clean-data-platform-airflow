from __future__ import annotations

import logging
import uuid
from typing import Any

from app.infrastructure.airflow_callbacks.compute_job_adapter import ComputeJobResult, JobStatus

logger = logging.getLogger(__name__)


class DbtComputeAdapter:
    """Simulates async dbt/Dataform transformation job lifecycle.

    Implements ComputeJobAdapter protocol. Each submitted job requires at least
    two poll cycles (simulating async CLI execution time) before completing.
    metrics_path is populated on SUCCESS with a local path for downstream tasks.

    Example:
        adapter = DbtComputeAdapter()
        job_id = adapter.submit_job("p-001", "etl", {"ref": "models/orders.sql"})
        result = adapter.poll_job_status(job_id)  # RUNNING
        result = adapter.poll_job_status(job_id)  # SUCCESS
    """

    def __init__(self) -> None:
        self._jobs: dict[str, int] = {}

    def submit_job(self, pipeline_id: str, pipeline_type: str, config: dict[str, Any]) -> str:
        """Register a new transform job and return its unique job_id."""
        job_id = f"dbt-job-{uuid.uuid4()}"
        self._jobs[job_id] = 0
        logger.info("dbt transform job submitted: %s (pipeline=%s)", job_id, pipeline_id)
        return job_id

    def poll_job_status(self, job_id: str) -> ComputeJobResult:
        """Poll job state. Returns RUNNING on first call, SUCCESS on second+, FAILED if unknown."""
        if job_id not in self._jobs:
            return ComputeJobResult(
                job_id=job_id, status=JobStatus.FAILED, error_message=f"Unknown job: {job_id}"
            )

        self._jobs[job_id] += 1
        attempts = self._jobs[job_id]

        if attempts < 2:
            return ComputeJobResult(job_id=job_id, status=JobStatus.RUNNING)

        metrics_path = f"/tmp/dbt_outputs/{job_id}/metrics.json"
        return ComputeJobResult(job_id=job_id, status=JobStatus.SUCCESS, metrics_path=metrics_path)

    def cancel_job(self, job_id: str) -> None:
        """Cancel and remove job state."""
        self._jobs.pop(job_id, None)
        logger.info("dbt transform job cancelled: %s", job_id)
