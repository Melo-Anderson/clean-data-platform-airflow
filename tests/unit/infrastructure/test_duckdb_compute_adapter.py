from __future__ import annotations

from concurrent.futures import Future

import pytest

from app.infrastructure.adapters.compute.job_state import JobState
from app.infrastructure.airflow_callbacks.compute_job_adapter import ComputeJobResult, JobStatus


# ---------------------------------------------------------------------------
# Tarefa 1: Testes do JobState
# ---------------------------------------------------------------------------


def test_job_state_initial_status_is_running() -> None:
    """JobState deve iniciar com os campos fornecidos e result/error como None."""
    future: Future[ComputeJobResult] = Future()
    state = JobState(job_id="abc-123", status=JobStatus.RUNNING, future=future)

    assert state.job_id == "abc-123"
    assert state.status == JobStatus.RUNNING
    assert state.result is None
    assert state.error is None


def test_job_state_holds_future_reference() -> None:
    """O future armazenado no JobState deve ser o mesmo objeto passado na construção."""
    future: Future[ComputeJobResult] = Future()
    state = JobState(job_id="xyz-999", status=JobStatus.RUNNING, future=future)

    assert state.future is future
