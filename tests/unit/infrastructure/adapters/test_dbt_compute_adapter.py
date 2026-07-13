from __future__ import annotations

from app.infrastructure.adapters.compute.dbt_compute_adapter import DbtComputeAdapter
from app.infrastructure.airflow_callbacks.compute_job_adapter import JobStatus


def test_submit_job_returns_dbt_prefixed_job_id() -> None:
    """submit_job deve retornar um job_id com prefixo 'dbt-job-'."""
    adapter = DbtComputeAdapter()
    job_id = adapter.submit_job(
        pipeline_id="p-001",
        pipeline_type="etl",
        config={"ref": "models/orders.sql", "engine": "dbt"},
    )
    assert job_id.startswith("dbt-job-")


def test_poll_job_status_is_running_on_first_poll() -> None:
    """Primeiro poll retorna RUNNING (simula job assíncrono em execução)."""
    adapter = DbtComputeAdapter()
    job_id = adapter.submit_job("p-001", "etl", {"ref": "models/x.sql"})
    result = adapter.poll_job_status(job_id)
    assert result.status == JobStatus.RUNNING


def test_poll_job_status_is_success_on_second_poll() -> None:
    """Segundo poll retorna SUCCESS com metrics_path preenchido."""
    adapter = DbtComputeAdapter()
    job_id = adapter.submit_job("p-001", "etl", {"ref": "models/x.sql"})
    adapter.poll_job_status(job_id)  # 1st: RUNNING
    result = adapter.poll_job_status(job_id)  # 2nd: SUCCESS
    assert result.status == JobStatus.SUCCESS
    assert result.metrics_path is not None
    assert job_id in result.metrics_path


def test_cancel_job_removes_job_state() -> None:
    """cancel_job elimina o estado do job; poll subsequente retorna FAILED."""
    adapter = DbtComputeAdapter()
    job_id = adapter.submit_job("p-001", "etl", {"ref": "models/x.sql"})
    adapter.cancel_job(job_id)
    result = adapter.poll_job_status(job_id)
    assert result.status == JobStatus.FAILED


def test_poll_unknown_job_returns_failed() -> None:
    """Poll de job inexistente retorna FAILED sem exceção."""
    adapter = DbtComputeAdapter()
    result = adapter.poll_job_status("non-existent-job-id")
    assert result.status == JobStatus.FAILED


def test_get_transform_adapter_returns_dbt_adapter_for_dbt_engine() -> None:
    """Factory deve retornar DbtComputeAdapter para engine 'dbt'."""
    from app.infrastructure.compute_job_factory import get_transform_adapter

    adapter = get_transform_adapter("dbt")
    assert isinstance(adapter, DbtComputeAdapter)
