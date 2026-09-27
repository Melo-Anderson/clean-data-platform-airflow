import pathlib
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

import pytest

from app.infrastructure.adapters.compute.duckdb_compute_adapter import DuckDbComputeAdapter
from app.infrastructure.adapters.compute.job_state import JobState
from app.infrastructure.adapters.compute.rest_api_compute_adapter import RestApiComputeAdapter
from app.infrastructure.airflow_callbacks.compute_job_adapter import ComputeJobResult, JobStatus
from app.infrastructure.compute_job_factory import get_compute_adapter

# ---------------------------------------------------------------------------
# Mock nomeado — sem MagicMock anônimo (regra do projeto)
# ---------------------------------------------------------------------------


class MockSecretManager:
    """Retorna credenciais fake sem I/O real."""

    async def resolve(self, ref: str) -> dict[str, str]:
        return {
            "driver": "postgres",
            "host": "localhost",
            "port": "5432",
            "database": "test_db",
            "user": "user",
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


# ---------------------------------------------------------------------------
# Tarefa 4: Factory
# ---------------------------------------------------------------------------


def test_factory_returns_duckdb_adapter_for_duckdb_engine() -> None:
    """get_compute_adapter('duckdb') deve retornar DuckDbComputeAdapter."""
    adapter = get_compute_adapter("duckdb")
    assert isinstance(adapter, DuckDbComputeAdapter)


def test_factory_raises_value_error_for_unknown_engine() -> None:
    """get_compute_adapter com engine desconhecido lanca ValueError (Fail-Fast)."""
    with pytest.raises(ValueError, match="Unsupported compute engine"):
        get_compute_adapter("spark")


def test_factory_returns_rest_api_adapter_for_rest_api_engine() -> None:
    """get_compute_adapter('rest_api') must return RestApiComputeAdapter."""
    adapter = get_compute_adapter("rest_api")
    assert isinstance(adapter, RestApiComputeAdapter)


def test_run_extraction_uses_extraction_query_when_provided(tmp_path: pathlib.Path) -> None:
    """_run_extraction deve usar extraction_query no lugar do SELECT * padrao."""
    adapter = DuckDbComputeAdapter(
        secret_manager=MockSecretManager(),
        output_base_dir=str(tmp_path),
    )
    executed_queries: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> "FakeConn":
            executed_queries.append(sql)
            return self

        def fetchone(self) -> tuple:
            return (42,)

        def fetchall(self) -> list:
            return [("id", "INTEGER", None, None, None, None, None)]

    with (
        patch("duckdb.connect", return_value=FakeConn()),
        patch.object(pathlib.Path, "stat", return_value=MagicMock(st_size=1024)),
    ):
        output_dir = tmp_path / "pipe-1" / "job-1"
        output_dir.mkdir(parents=True, exist_ok=True)
        adapter._run_extraction(
            job_id="job-1",
            config={
                "source_objects": [
                    {
                        "object_name": "demo_orders",
                        "credential_ref": "secret/postgres",
                        "extraction_query": "SELECT id, amount FROM demo_orders WHERE amount > 0",
                    }
                ]
            },
            output_dir=output_dir,
        )

    copy_calls = [q for q in executed_queries if "COPY" in q]
    assert len(copy_calls) == 1
    assert "SELECT id, amount FROM demo_orders WHERE amount > 0" in copy_calls[0]
    assert "SELECT * FROM" not in copy_calls[0]


def test_run_extraction_uses_default_select_when_no_query(tmp_path: pathlib.Path) -> None:
    """_run_extraction usa SELECT * FROM source_db.public.{table} quando extraction_query e None."""
    adapter = DuckDbComputeAdapter(
        secret_manager=MockSecretManager(),
        output_base_dir=str(tmp_path),
    )
    executed_queries: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> "FakeConn":
            executed_queries.append(sql)
            return self

        def fetchone(self) -> tuple:
            return (10,)

        def fetchall(self) -> list:
            return [("id", "INTEGER", None, None, None, None, None)]

    with (
        patch("duckdb.connect", return_value=FakeConn()),
        patch.object(pathlib.Path, "stat", return_value=MagicMock(st_size=512)),
    ):
        output_dir = tmp_path / "pipe-2" / "job-2"
        output_dir.mkdir(parents=True, exist_ok=True)
        adapter._run_extraction(
            job_id="job-2",
            config={
                "source_objects": [
                    {
                        "object_name": "demo_customers",
                        "credential_ref": "secret/postgres",
                    }
                ]
            },
            output_dir=output_dir,
        )

    copy_calls = [q for q in executed_queries if "COPY" in q]
    assert len(copy_calls) == 1
    assert "SELECT * FROM source_db.public.demo_customers" in copy_calls[0]


def test_duckdb_adapter_requires_explicit_credential_ref(tmp_path: pathlib.Path) -> None:
    from unittest.mock import AsyncMock

    secret_mgr = AsyncMock()
    secret_mgr.resolve.return_value = {"driver": "sqlite", "database": ":memory:"}

    adapter = DuckDbComputeAdapter(
        secret_manager=secret_mgr,
        output_base_dir=str(tmp_path),
        default_credential_ref="secret/default_db",
    )

    assert adapter._default_credential_ref == "secret/default_db"


def test_duckdb_adapter_constructor_does_not_accept_postgres_host_override(
    tmp_path: pathlib.Path,
) -> None:
    """postgres_host_override must be removed — host comes from Secret Manager only."""
    import inspect

    sig = inspect.signature(DuckDbComputeAdapter.__init__)
    assert "postgres_host_override" not in sig.parameters


def test_poll_job_status_falls_back_to_disk_when_not_in_active_jobs(tmp_path: pathlib.Path) -> None:
    """Quando o job não está em _active_jobs (outro processo do Airflow), deve ler o status do disco."""
    output_base = tmp_path / "duckdb_outputs"
    job_dir = output_base / "pipe-1" / "job-disk-1"
    job_dir.mkdir(parents=True)
    (job_dir / "data.parquet").write_bytes(b"fake parquet")
    (job_dir / "metrics.json").write_text('{"row_count": 42}', encoding="utf-8")

    adapter = DuckDbComputeAdapter(
        secret_manager=MockSecretManager(),
        output_base_dir=str(output_base),
    )
    result = adapter.poll_job_status("job-disk-1")
    assert result.status == JobStatus.SUCCESS
    assert result.output_path == str(job_dir / "data.parquet")
    assert result.metrics_path == str(job_dir / "metrics.json")


def test_poll_job_status_reads_error_txt_from_disk(tmp_path: pathlib.Path) -> None:
    """Quando o job falhou e error.txt está no disco, deve retornar FAILED com a mensagem do erro."""
    output_base = tmp_path / "duckdb_outputs"
    job_dir = output_base / "pipe-1" / "job-disk-err"
    job_dir.mkdir(parents=True)
    (job_dir / "error.txt").write_text("Table financial_report does not exist", encoding="utf-8")

    adapter = DuckDbComputeAdapter(
        secret_manager=MockSecretManager(),
        output_base_dir=str(output_base),
    )
    result = adapter.poll_job_status("job-disk-err")
    assert result.status == JobStatus.FAILED
    assert "Table financial_report does not exist" in (result.error_message or "")
