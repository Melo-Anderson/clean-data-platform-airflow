from __future__ import annotations

import threading
from concurrent.futures import Future
from typing import Any

from app.infrastructure.adapters.compute.job_state import JobState
from app.infrastructure.adapters.compute.rest_api_compute_adapter import RestApiComputeAdapter
from app.infrastructure.airflow_callbacks.compute_job_adapter import ComputeJobResult, JobStatus

# ---------------------------------------------------------------------------
# Mock nomeado — sem MagicMock anônimo (regra do projeto)
# ---------------------------------------------------------------------------


class MockSecretManager:
    """Retorna credenciais fake sem I/O real."""

    async def resolve(self, ref: str) -> dict[str, str]:
        return {"token": "fake-token"}


# ---------------------------------------------------------------------------
# submit_job
# ---------------------------------------------------------------------------


def test_submit_job_returns_uuid_string(tmp_path: Any) -> None:
    """submit_job retorna um UUID v4 string imediatamente."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(),
        output_base_dir=str(tmp_path),
    )
    try:
        job_id = adapter.submit_job(
            pipeline_id="pipe-1",
            pipeline_type="ingestion",
            config={
                "credential_ref": "secret/api",
                "base_url": "https://api.example.com",
                "resource_path": "/products",
            },
        )
        assert isinstance(job_id, str)
        assert len(job_id) == 36  # UUID v4
    finally:
        adapter.shutdown()


def test_submit_job_registers_in_active_jobs(tmp_path: Any) -> None:
    """submit_job deve registrar o job em _active_jobs com status RUNNING."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(),
        output_base_dir=str(tmp_path),
    )
    try:
        job_id = adapter.submit_job(
            pipeline_id="pipe-1",
            pipeline_type="ingestion",
            config={
                "credential_ref": "secret/api",
                "base_url": "https://api.example.com",
                "resource_path": "/products",
            },
        )
        assert job_id in adapter._active_jobs
        assert adapter._active_jobs[job_id].status == JobStatus.RUNNING
    finally:
        adapter.shutdown()


# ---------------------------------------------------------------------------
# poll_job_status
# ---------------------------------------------------------------------------


def test_poll_unknown_job_returns_failed(tmp_path: Any) -> None:
    """poll_job_status com job_id desconhecido retorna FAILED."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(),
        output_base_dir=str(tmp_path),
    )
    try:
        result = adapter.poll_job_status("nao-existe")
        assert result.status == JobStatus.FAILED
        assert "nao-existe" in (result.error_message or "")
    finally:
        adapter.shutdown()


def test_poll_returns_running_while_future_pending(tmp_path: Any) -> None:
    """poll_job_status retorna RUNNING enquanto a future está pendente."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(),
        output_base_dir=str(tmp_path),
    )
    try:
        future: Future[ComputeJobResult] = Future()
        adapter._active_jobs["pending-job"] = JobState(
            job_id="pending-job", status=JobStatus.RUNNING, future=future
        )
        result = adapter.poll_job_status("pending-job")
        assert result.status == JobStatus.RUNNING
    finally:
        adapter.shutdown()


def test_poll_returns_success_and_evicts_completed_job(tmp_path: Any) -> None:
    """poll_job_status retorna SUCCESS e remove o job de _active_jobs (evição)."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(),
        output_base_dir=str(tmp_path),
    )
    try:
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
        assert "test-job" not in adapter._active_jobs  # evicted
    finally:
        adapter.shutdown()


def test_poll_returns_failed_and_evicts_on_exception(tmp_path: Any) -> None:
    """poll_job_status retorna FAILED e remove o job quando a future lança exceção."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(),
        output_base_dir=str(tmp_path),
    )
    try:
        future: Future[ComputeJobResult] = Future()
        future.set_exception(RuntimeError("Connection refused"))
        adapter._active_jobs["fail-job"] = JobState(
            job_id="fail-job", status=JobStatus.RUNNING, future=future
        )
        result = adapter.poll_job_status("fail-job")
        assert result.status == JobStatus.FAILED
        assert "Connection refused" in (result.error_message or "")
        assert "fail-job" not in adapter._active_jobs  # evicted
    finally:
        adapter.shutdown()


# ---------------------------------------------------------------------------
# cancel_job
# ---------------------------------------------------------------------------


def test_cancel_job_cancels_future(tmp_path: Any) -> None:
    """cancel_job deve cancelar a future pendente."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(),
        output_base_dir=str(tmp_path),
    )
    try:
        future: Future[ComputeJobResult] = Future()
        adapter._active_jobs["cancel-job"] = JobState(
            job_id="cancel-job", status=JobStatus.RUNNING, future=future
        )
        adapter.cancel_job("cancel-job")
        assert future.cancelled()
    finally:
        adapter.shutdown()


# ---------------------------------------------------------------------------
# _build_auth_headers
# ---------------------------------------------------------------------------


def test_build_auth_headers_bearer(tmp_path: Any) -> None:
    """Bearer auth: retorna Authorization header."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(), output_base_dir=str(tmp_path)
    )
    try:
        headers = adapter._build_auth_headers("bearer", {"token": "mytoken"})
        assert headers == {"Authorization": "Bearer mytoken"}
    finally:
        adapter.shutdown()


def test_build_auth_headers_api_key(tmp_path: Any) -> None:
    """API key auth: retorna x-api-key header."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(), output_base_dir=str(tmp_path)
    )
    try:
        headers = adapter._build_auth_headers("api_key", {"api_key": "mykey"})
        assert headers == {"x-api-key": "mykey"}
    finally:
        adapter.shutdown()


def test_build_auth_headers_basic(tmp_path: Any) -> None:
    """Basic auth: retorna Authorization Basic header (base64 encoded)."""
    import base64

    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(), output_base_dir=str(tmp_path)
    )
    try:
        headers = adapter._build_auth_headers("basic", {"username": "user", "password": "pass"})
        expected = base64.b64encode(b"user:pass").decode()
        assert headers == {"Authorization": f"Basic {expected}"}
    finally:
        adapter.shutdown()


def test_build_auth_headers_unknown_returns_empty(tmp_path: Any) -> None:
    """Auth type desconhecido retorna dict vazio."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(), output_base_dir=str(tmp_path)
    )
    try:
        headers = adapter._build_auth_headers("oauth2_magic", {"token": "x"})
        assert headers == {}
    finally:
        adapter.shutdown()


# ---------------------------------------------------------------------------
# _resolve_jsonpath
# ---------------------------------------------------------------------------


def test_resolve_jsonpath_simple_key(tmp_path: Any) -> None:
    """_resolve_jsonpath resolve chave simples."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(), output_base_dir=str(tmp_path)
    )
    try:
        assert adapter._resolve_jsonpath({"next_cursor": "abc123"}, "next_cursor") == "abc123"
    finally:
        adapter.shutdown()


def test_resolve_jsonpath_nested_path(tmp_path: Any) -> None:
    """_resolve_jsonpath resolve paths aninhados com '.'."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(), output_base_dir=str(tmp_path)
    )
    try:
        data = {"pagination": {"next_cursor": "page2"}}
        assert adapter._resolve_jsonpath(data, "pagination.next_cursor") == "page2"
    finally:
        adapter.shutdown()


def test_resolve_jsonpath_missing_returns_none(tmp_path: Any) -> None:
    """_resolve_jsonpath retorna None para paths inexistentes."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(), output_base_dir=str(tmp_path)
    )
    try:
        assert adapter._resolve_jsonpath({}, "pagination.cursor") is None
    finally:
        adapter.shutdown()


# ---------------------------------------------------------------------------
# Thread safety (stress test)
# ---------------------------------------------------------------------------


def test_concurrent_poll_does_not_raise(tmp_path: Any) -> None:
    """Acessos concorrentes a poll_job_status não devem causar RuntimeError."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(),
        output_base_dir=str(tmp_path),
        max_workers=4,
    )
    try:
        errors: list[Exception] = []

        def _poll() -> None:
            try:
                for _ in range(50):
                    adapter.poll_job_status("non-existent")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=_poll) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == [], f"Thread safety errors: {errors}"
    finally:
        adapter.shutdown()
