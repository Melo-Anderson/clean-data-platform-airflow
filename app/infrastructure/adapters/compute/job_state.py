from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass

from app.infrastructure.airflow_callbacks.compute_job_adapter import ComputeJobResult, JobStatus


@dataclass
class JobState:
    """
    Rastreia o estado de um job DuckDB em execução no background.

    Não é frozen porque status e result são consultados após a future concluir.
    Uma instância por job_id em DuckDbComputeAdapter._active_jobs.
    """

    job_id: str
    status: JobStatus
    future: Future[ComputeJobResult]
    result: ComputeJobResult | None = None
    error: str | None = None
