from __future__ import annotations

from concurrent.futures import Future

from app.infrastructure.adapters.compute.job_state import JobState
from app.infrastructure.adapters.compute.rest_api_compute_adapter import RestApiComputeAdapter
from app.infrastructure.airflow_callbacks.compute_job_adapter import ComputeJobResult, JobStatus


class MockSecretManager:
    """Returns fake credentials without real I/O."""

    async def resolve(self, ref: str) -> dict[str, str]:
        return {"token": "fake-token", "api_key": "fake-key"}


_BASE_CONFIG = {
    "base_url": "http://api.test",
    "resource_path": "/items",
    "credential_ref": "vault/api/test",
    "auth_type": "bearer",
    "pagination": {"strategy": "none"},
}


def test_submit_returns_uuid_and_registers_job(tmp_path: Path) -> None:
    """submit_job must return a UUID v4 string and register the job as RUNNING."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(), output_base_dir=str(tmp_path)
    )
    job_id = adapter.submit_job("pipe-1", "rest_api_ingestion", _BASE_CONFIG)
    assert isinstance(job_id, str)
    assert len(job_id) == 36  # UUID v4
    assert job_id in adapter._active_jobs
    assert adapter._active_jobs[job_id].status == JobStatus.RUNNING


def test_poll_returns_running_while_future_pending() -> None:
    """poll_job_status returns RUNNING when the background Future is not yet done."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(), output_base_dir="/tmp/test_rest_api"
    )
    future: Future[ComputeJobResult] = Future()
    adapter._active_jobs["pending-job"] = JobState(
        job_id="pending-job", status=JobStatus.RUNNING, future=future
    )
    result = adapter.poll_job_status("pending-job")
    assert result.status == JobStatus.RUNNING


def test_poll_returns_success_after_future_completes() -> None:
    """poll_job_status returns SUCCESS with all paths when the Future resolves."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(), output_base_dir="/tmp/test_rest_api"
    )
    future: Future[ComputeJobResult] = Future()
    expected = ComputeJobResult(
        job_id="done-job",
        status=JobStatus.SUCCESS,
        output_path="/tmp/data.parquet",
        metrics_path="/tmp/metrics.json",
        schema_path="/tmp/schema.json",
    )
    future.set_result(expected)
    adapter._active_jobs["done-job"] = JobState(
        job_id="done-job", status=JobStatus.RUNNING, future=future
    )
    result = adapter.poll_job_status("done-job")
    assert result.status == JobStatus.SUCCESS
    assert result.output_path == "/tmp/data.parquet"
    assert result.metrics_path == "/tmp/metrics.json"


def test_poll_returns_failed_when_future_raises() -> None:
    """poll_job_status returns FAILED with error_message when the thread raises."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(), output_base_dir="/tmp/test_rest_api"
    )
    future: Future[ComputeJobResult] = Future()
    future.set_exception(RuntimeError("Connection refused by API"))
    adapter._active_jobs["fail-job"] = JobState(
        job_id="fail-job", status=JobStatus.RUNNING, future=future
    )
    result = adapter.poll_job_status("fail-job")
    assert result.status == JobStatus.FAILED
    assert "Connection refused by API" in (result.error_message or "")


def test_poll_returns_failed_for_unknown_job_id() -> None:
    """poll_job_status returns FAILED with a descriptive message for unknown job_id."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(), output_base_dir="/tmp/test_rest_api"
    )
    result = adapter.poll_job_status("does-not-exist")
    assert result.status == JobStatus.FAILED
    assert "does-not-exist" in (result.error_message or "")


def test_cancel_job_cancels_future() -> None:
    """cancel_job must call future.cancel() for a known job_id."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(), output_base_dir="/tmp/test_rest_api"
    )
    future: Future[ComputeJobResult] = Future()
    adapter._active_jobs["cancel-job"] = JobState(
        job_id="cancel-job", status=JobStatus.RUNNING, future=future
    )
    adapter.cancel_job("cancel-job")
    assert future.cancelled()


from pathlib import Path

import httpx
import pytest
import respx


@pytest.mark.asyncio
async def test_extract_async_offset_limit_pagination(tmp_path: Path) -> None:
    """offset_limit: fetches 2 pages, stops when second page is smaller than page_size."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(), output_base_dir=str(tmp_path)
    )
    config = {
        "base_url": "http://api.test",
        "resource_path": "/items",
        "credential_ref": "vault/api/test",
        "auth_type": "bearer",
        "pagination": {
            "strategy": "offset_limit",
            "page_size": 2,
            "limit_param": "limit",
            "offset_param": "offset",
        },
    }
    with respx.mock:
        respx.get("http://api.test/items", params={"limit": 2, "offset": 0}).respond(
            200, json={"data": [{"id": 1}, {"id": 2}]}
        )
        respx.get("http://api.test/items", params={"limit": 2, "offset": 2}).respond(
            200, json={"data": [{"id": 3}]}
        )
        await adapter._extract_async("job-1", config, tmp_path / "job-1")


@pytest.mark.asyncio
async def test_extract_async_cursor_pagination(tmp_path: Path) -> None:
    """cursor: fetches pages until next_cursor is null."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(), output_base_dir=str(tmp_path)
    )
    config = {
        "base_url": "http://api.test",
        "resource_path": "/items",
        "credential_ref": "vault/api/test",
        "auth_type": "",
        "pagination": {
            "strategy": "cursor",
            "page_size": 2,
            "cursor_jsonpath": "next_cursor",
        },
    }
    with respx.mock:
        # Note: respx evaluates mocks in reverse order of definition.
        # The specific match (with params) MUST be defined first (which evaluates last)
        # or the generic match will catch everything and cause an infinite loop!
        respx.get("http://api.test/items", params={"cursor": "tok-2"}).respond(
            200, json={"data": [{"id": 3}], "next_cursor": None}
        )
        respx.get("http://api.test/items").respond(
            200, json={"data": [{"id": 1}, {"id": 2}], "next_cursor": "tok-2"}
        )
        await adapter._extract_async("job-2", config, tmp_path / "job-2")


@pytest.mark.asyncio
async def test_extract_async_single_page(tmp_path: Path) -> None:
    """strategy none: makes exactly 1 HTTP request."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(), output_base_dir=str(tmp_path)
    )
    config = {
        "base_url": "http://api.test",
        "resource_path": "/report",
        "credential_ref": "vault/api/test",
        "auth_type": "api_key",
        "pagination": {"strategy": "none"},
    }
    with respx.mock:
        route = respx.get("http://api.test/report").respond(200, json=[{"total": 42}])
        await adapter._extract_async("job-3", config, tmp_path / "job-3")
    assert route.call_count == 1


@pytest.mark.asyncio
async def test_extract_async_page_number_pagination(tmp_path: Path) -> None:
    """strategy page_number: fetches pages incrementing page parameter until response size < page_size."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(), output_base_dir=str(tmp_path)
    )
    config = {
        "base_url": "http://api.test",
        "resource_path": "/items",
        "credential_ref": "vault/api/test",
        "auth_type": "",
        "pagination": {
            "strategy": "page_number",
            "page_size": 2,
            "limit_param": "limit",
            "page_param": "page",
            "page_start": 1,
        },
    }
    with respx.mock:
        route_page2 = respx.get(
            "http://api.test/items", params={"limit": "2", "page": "2"}
        ).respond(200, json=[{"id": 3}])
        route_page1 = respx.get(
            "http://api.test/items", params={"limit": "2", "page": "1"}
        ).respond(200, json=[{"id": 1}, {"id": 2}])
        await adapter._extract_async("job-p1", config, tmp_path / "job-p1")
    assert route_page2.call_count == 1
    assert route_page1.call_count == 1


@pytest.mark.asyncio
async def test_extract_async_bearer_header_is_set(tmp_path: Path) -> None:
    """Bearer auth_type must set Authorization header on every request."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(), output_base_dir=str(tmp_path)
    )
    config = {
        "base_url": "http://api.test",
        "resource_path": "/secure",
        "credential_ref": "vault/api/test",
        "auth_type": "bearer",
        "pagination": {"strategy": "none"},
    }
    with respx.mock:
        route = respx.get("http://api.test/secure").respond(200, json=[{"x": 1}])
        await adapter._extract_async("job-4", config, tmp_path / "job-4")
    assert route.calls[0].request.headers["authorization"] == "Bearer fake-token"


@pytest.mark.asyncio
async def test_extract_async_unwraps_envelope(tmp_path: Path) -> None:
    """JSON envelope with key 'data' must be unwrapped to extract the list."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(), output_base_dir=str(tmp_path)
    )
    config = {
        "base_url": "http://api.test",
        "resource_path": "/wrapped",
        "credential_ref": "vault/api/test",
        "auth_type": "",
        "pagination": {"strategy": "none"},
    }
    with respx.mock:
        respx.get("http://api.test/wrapped").respond(
            200, json={"data": [{"id": 1, "name": "A"}], "total": 1}
        )
        await adapter._extract_async("job-5", config, tmp_path / "job-5")
    import pyarrow.parquet as pq

    table = pq.read_table(tmp_path / "job-5" / "data.parquet")
    assert table.num_rows == 1


import json as _json


@pytest.mark.asyncio
async def test_extract_async_writes_parquet_metrics_schema(tmp_path: Path) -> None:
    """_extract_async must create data.parquet, metrics.json, and schema.json with correct content."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(), output_base_dir=str(tmp_path)
    )
    config = {
        "base_url": "http://api.test",
        "resource_path": "/products",
        "credential_ref": "vault/api/test",
        "auth_type": "",
        "pagination": {"strategy": "none"},
    }
    output_dir = tmp_path / "job-out"
    output_dir.mkdir()

    with respx.mock:
        respx.get("http://api.test/products").respond(
            200,
            json=[
                {"id": 1, "name": "Widget", "price": 9.99},
                {"id": 2, "name": "Gadget", "price": 19.99},
            ],
        )
        await adapter._extract_async("job-out", config, output_dir)

    # data.parquet
    assert (output_dir / "data.parquet").exists()
    import pyarrow.parquet as pq

    table = pq.read_table(output_dir / "data.parquet")
    assert table.num_rows == 2
    assert table.column_names == ["id", "name", "price"]

    # metrics.json
    assert (output_dir / "metrics.json").exists()
    metrics = _json.loads((output_dir / "metrics.json").read_text(encoding="utf-8"))
    assert metrics["row_count"] == 2
    assert metrics["bytes_written"] > 0
    assert metrics["pages_fetched"] == 1

    # schema.json
    assert (output_dir / "schema.json").exists()
    schema = _json.loads((output_dir / "schema.json").read_text(encoding="utf-8"))
    assert any(col["column"] == "id" for col in schema)
    assert any(col["column"] == "name" for col in schema)


@pytest.mark.asyncio
async def test_extract_async_retries_on_429(tmp_path: Path) -> None:
    """_extract_async must retry automatically when API returns 429 Too Many Requests."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(), output_base_dir=str(tmp_path)
    )
    config = {
        "base_url": "http://api.test",
        "resource_path": "/retry-me",
        "credential_ref": "vault/api/test",
        "auth_type": "",
        "pagination": {"strategy": "none"},
    }
    with respx.mock:
        route = respx.get("http://api.test/retry-me")
        route.side_effect = [
            httpx.Response(429),
            httpx.Response(200, json=[{"id": 99}]),
        ]
        await adapter._extract_async("job-retry", config, tmp_path / "job-retry")
    assert route.call_count == 2
