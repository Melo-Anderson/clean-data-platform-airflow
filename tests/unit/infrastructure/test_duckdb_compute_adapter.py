from __future__ import annotations

from concurrent.futures import Future

from app.infrastructure.adapters.compute.duckdb_compute_adapter import DuckDbComputeAdapter
from app.infrastructure.adapters.compute.job_state import JobState
from app.infrastructure.airflow_callbacks.compute_job_adapter import ComputeJobResult, JobStatus


# ---------------------------------------------------------------------------
# Mock nomeado — sem MagicMock anônimo (regra do projeto)
# ---------------------------------------------------------------------------


class MockSecretManager:
    """Retorna credenciais fake sem I/O real."""

    async def resolve(self, ref: str) -> dict[str, str]:
        return {
            "host": "localhost",
            "port": "5432",
            "dbname": "test_db",
            "username": "user",
            "password": "pass",
        }


# ---------------------------------------------------------------------------
# Tarefa 1: JobState
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
    """O future armazenado deve ser o mesmo objeto passado na construção."""
    future: Future[ComputeJobResult] = Future()
    state = JobState(job_id="xyz-999", status=JobStatus.RUNNING, future=future)

    assert state.future is future


# ---------------------------------------------------------------------------
# Tarefa 2: submit_job e poll_job_status
# ---------------------------------------------------------------------------


def test_submit_job_returns_uuid_string() -> None:
    """submit_job retorna um UUID v4 string imediatamente."""
    adapter = DuckDbComputeAdapter(
        secret_manager=MockSecretManager(),
        output_base_dir="/tmp/test_duckdb",
    )
    job_id = adapter.submit_job(
        pipeline_id="pipe-1",
        pipeline_type="ingestion",
        config={"credential_ref": "secret/postgres", "source_table": "orders"},
    )
    assert isinstance(job_id, str)
    assert len(job_id) == 36  # UUID v4


def test_submit_job_registers_in_active_jobs() -> None:
    """submit_job deve registrar o job em _active_jobs com status RUNNING."""
    adapter = DuckDbComputeAdapter(
        secret_manager=MockSecretManager(),
        output_base_dir="/tmp/test_duckdb",
    )
    job_id = adapter.submit_job(
        pipeline_id="pipe-1",
        pipeline_type="ingestion",
        config={"credential_ref": "secret/postgres", "source_table": "orders"},
    )
    assert job_id in adapter._active_jobs
    assert adapter._active_jobs[job_id].status == JobStatus.RUNNING


def test_poll_unknown_job_returns_failed() -> None:
    """poll_job_status com job_id desconhecido retorna FAILED com mensagem clara."""
    adapter = DuckDbComputeAdapter(
        secret_manager=MockSecretManager(),
        output_base_dir="/tmp/test_duckdb",
    )
    result = adapter.poll_job_status("nao-existe")

    assert result.status == JobStatus.FAILED
    assert "nao-existe" in (result.error_message or "")


# ---------------------------------------------------------------------------
# Tarefa 3: Transições de estado após conclusão da thread
# ---------------------------------------------------------------------------


def test_poll_returns_success_after_future_completes() -> None:
    """poll_job_status retorna SUCCESS com paths quando a future termina com êxito."""
    adapter = DuckDbComputeAdapter(
        secret_manager=MockSecretManager(),
        output_base_dir="/tmp/test_duckdb",
    )

    future: Future[ComputeJobResult] = Future()
    expected = ComputeJobResult(
        job_id="test-job",
        status=JobStatus.SUCCESS,
        output_path="/tmp/data.parquet",
        metrics_path="/tmp/metrics.json",
        schema_path="/tmp/schema.json",
    )
    future.set_result(expected)

    adapter._active_jobs["test-job"] = JobState(
        job_id="test-job", status=JobStatus.RUNNING, future=future
    )

    result = adapter.poll_job_status("test-job")

    assert result.status == JobStatus.SUCCESS
    assert result.output_path == "/tmp/data.parquet"
    assert result.metrics_path == "/tmp/metrics.json"


def test_poll_returns_failed_when_future_raises() -> None:
    """poll_job_status retorna FAILED com a mensagem de erro quando a thread lança exceção."""
    adapter = DuckDbComputeAdapter(
        secret_manager=MockSecretManager(),
        output_base_dir="/tmp/test_duckdb",
    )

    future: Future[ComputeJobResult] = Future()
    future.set_exception(RuntimeError("Conexão com banco recusada"))

    adapter._active_jobs["fail-job"] = JobState(
        job_id="fail-job", status=JobStatus.RUNNING, future=future
    )

    result = adapter.poll_job_status("fail-job")

    assert result.status == JobStatus.FAILED
    assert "Conexão com banco recusada" in (result.error_message or "")


def test_poll_returns_running_while_future_pending() -> None:
    """poll_job_status retorna RUNNING enquanto a thread ainda não terminou."""
    adapter = DuckDbComputeAdapter(
        secret_manager=MockSecretManager(),
        output_base_dir="/tmp/test_duckdb",
    )

    future: Future[ComputeJobResult] = Future()  # nunca resolvido

    adapter._active_jobs["pending-job"] = JobState(
        job_id="pending-job", status=JobStatus.RUNNING, future=future
    )

    result = adapter.poll_job_status("pending-job")

    assert result.status == JobStatus.RUNNING
