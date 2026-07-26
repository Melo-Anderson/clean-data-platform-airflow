from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass

from app.infrastructure.airflow_callbacks.compute_job_adapter import ComputeJobResult, JobStatus


@dataclass
class JobState:
    """
    Tracks the state of a background compute job.

    Shared by DuckDbComputeAdapter and RestApiComputeAdapter.
    One instance per job_id in each adapter's _active_jobs dict.
    Jobs are evicted from _active_jobs after polling returns a terminal state.
    """

    job_id: str
    status: JobStatus
    future: Future[ComputeJobResult]
    result: ComputeJobResult | None = None
    error: str | None = None
