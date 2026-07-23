# REST API Compute Adapter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `RestApiComputeAdapter` to extract paginated JSON from REST API endpoints and write Parquet output files with metrics and schema metadata, following the exact same `ComputeJobAdapter` pattern as `DuckDbComputeAdapter`.

**Architecture:** A synchronous `ComputeJobAdapter` facade that dispatches work to a `ThreadPoolExecutor`. Inside the thread, `asyncio.run(_extract_async(...))` performs secret resolution, HTTP pagination via `httpx.AsyncClient`, envelope unwrapping, retry logic via `tenacity`, and chunked Parquet streaming via `pyarrow.parquet.ParquetWriter`.

**Tech Stack:** Python 3.12, `httpx` (already in project), `pyarrow` (already in project), `tenacity` (already in project, v9.1.4).

## Global Constraints

- Implement `ComputeJobAdapter` protocol — same three public methods: `submit_job`, `poll_job_status`, `cancel_job`.
- HTTP and secret resolution MUST run inside the background thread (never in the Airflow worker thread's event loop).
- No `MagicMock` in tests — use named mock classes following project conventions (see `test_duckdb_compute_adapter.py`).
- No `time.sleep()` in tests — manipulate `Future` directly to assert state transitions.
- Factory registration uses engine key `"rest_api"` via the existing `get_compute_adapter` function.
- `from __future__ import annotations` at the top of all new Python files.

---

### Task 1: JobState reuse + Component Scaffold

**Files:**
- Create: `app/infrastructure/adapters/compute/rest_api_compute_adapter.py`
- Modify: `app/infrastructure/compute_job_factory.py`
- Create: `tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py`

**Interfaces:**
- Consumes: `JobState` (`app/infrastructure/adapters/compute/job_state.py`), `ComputeJobAdapter` protocol (`app/infrastructure/airflow_callbacks/compute_job_adapter.py`), `SecretManagerPort` (`app/application/shared/secret_manager_port.py`).
- Produces: `RestApiComputeAdapter(secret_manager, output_base_dir, max_workers)` implementing `submit_job → str`, `poll_job_status → ComputeJobResult`, `cancel_job → None`.

- [ ] **Step 1: Write failing tests for submit/poll/cancel lifecycle**

```python
# tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py
from __future__ import annotations

from concurrent.futures import Future

from app.infrastructure.adapters.compute.rest_api_compute_adapter import RestApiComputeAdapter
from app.infrastructure.adapters.compute.job_state import JobState
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


def test_submit_returns_uuid_and_registers_job() -> None:
    """submit_job must return a UUID v4 string and register the job as RUNNING."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(), output_base_dir="/tmp/test_rest_api"
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
```

- [ ] **Step 2: Run tests to verify they fail**
```bash
uv run pytest tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py -v
```
Expected: FAIL — `ModuleNotFoundError: No module named 'app.infrastructure.adapters.compute.rest_api_compute_adapter'`

- [ ] **Step 3: Implement `RestApiComputeAdapter` scaffold**

```python
# app/infrastructure/adapters/compute/rest_api_compute_adapter.py
from __future__ import annotations

import asyncio
import logging
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Any

from app.application.shared.secret_manager_port import SecretManagerPort
from app.infrastructure.adapters.compute.job_state import JobState
from app.infrastructure.airflow_callbacks.compute_job_adapter import ComputeJobResult, JobStatus

logger = logging.getLogger(__name__)


class RestApiComputeAdapter:
    """
    Compute adapter for REST API ingestion pipelines.

    Implements the same submit → poll → cancel contract as DuckDbComputeAdapter.
    HTTP extraction, pagination, and Parquet writing run inside a background
    ThreadPoolExecutor so the Airflow worker thread is never blocked.

    Secret resolution uses asyncio.run() inside the worker thread because:
    - submit_job is synchronous (Protocol does not allow async)
    - Threads do not inherit the Airflow event loop
    - asyncio.run() creates an isolated event loop per call
    """

    def __init__(
        self,
        secret_manager: SecretManagerPort,
        output_base_dir: str = "/tmp/rest_api_outputs",
        max_workers: int = 4,
    ) -> None:
        self._secret_manager = secret_manager
        self._output_base_dir = Path(output_base_dir)
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._active_jobs: dict[str, JobState] = {}

    def submit_job(
        self,
        pipeline_id: str,
        pipeline_type: str,
        config: dict[str, Any],
    ) -> str:
        """Submit extraction job to background thread. Returns job_id immediately."""
        job_id = str(uuid.uuid4())
        output_dir = self._output_base_dir / pipeline_id / job_id
        output_dir.mkdir(parents=True, exist_ok=True)

        future: Future[ComputeJobResult] = self._executor.submit(
            self._run_extraction,
            job_id=job_id,
            config=config,
            output_dir=output_dir,
        )
        self._active_jobs[job_id] = JobState(
            job_id=job_id,
            status=JobStatus.RUNNING,
            future=future,
        )
        logger.info("RestApi job submitted: %s (pipeline=%s)", job_id, pipeline_id)
        return job_id

    def poll_job_status(self, job_id: str) -> ComputeJobResult:
        """Check job state. Returns RUNNING while thread executes; SUCCESS or FAILED on completion."""
        state = self._active_jobs.get(job_id)
        if state is None:
            return ComputeJobResult(
                job_id=job_id,
                status=JobStatus.FAILED,
                error_message=f"Unknown job_id: {job_id}",
            )
        if not state.future.done():
            return ComputeJobResult(job_id=job_id, status=JobStatus.RUNNING)
        exc = state.future.exception()
        if exc is not None:
            return ComputeJobResult(
                job_id=job_id,
                status=JobStatus.FAILED,
                error_message=str(exc),
            )
        return state.future.result()

    def cancel_job(self, job_id: str) -> None:
        """Cancel a running job. Called by the DAG's on_failure_callback."""
        state = self._active_jobs.get(job_id)
        if state is not None:
            state.future.cancel()
            logger.info("RestApi job cancelled: %s", job_id)

    def _run_extraction(
        self,
        job_id: str,
        config: dict[str, Any],
        output_dir: Path,
    ) -> ComputeJobResult:
        """Run async extraction inside background thread via isolated event loop."""
        asyncio.run(self._extract_async(job_id=job_id, config=config, output_dir=output_dir))
        return ComputeJobResult(
            job_id=job_id,
            status=JobStatus.SUCCESS,
            output_path=str(output_dir / "data.parquet"),
            metrics_path=str(output_dir / "metrics.json"),
            schema_path=str(output_dir / "schema.json"),
        )

    async def _extract_async(
        self,
        job_id: str,
        config: dict[str, Any],
        output_dir: Path,
    ) -> None:
        """To be implemented in Task 3."""
        pass
```

- [ ] **Step 4: Register in factory**

```python
# app/infrastructure/compute_job_factory.py
# Add this branch inside get_compute_adapter, before the final return:

    if engine == "rest_api":
        from app.config import get_settings
        from app.infrastructure.adapters.compute.rest_api_compute_adapter import RestApiComputeAdapter
        from app.infrastructure.adapters.secrets.secret_manager_factory import get_secret_manager

        return RestApiComputeAdapter(secret_manager=get_secret_manager(get_settings()))
```

- [ ] **Step 5: Run tests to verify they pass**
```bash
uv run pytest tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py -v
```
Expected: 6 tests PASS.

- [ ] **Step 6: Commit**
```bash
git add app/infrastructure/adapters/compute/rest_api_compute_adapter.py app/infrastructure/compute_job_factory.py tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py
git commit -m "feat: scaffold RestApiComputeAdapter with JobState lifecycle and factory registration"
```

---

### Task 2: Factory test

**Files:**
- Modify: `tests/unit/infrastructure/test_duckdb_compute_adapter.py`

**Interfaces:**
- Consumes: `get_compute_adapter` from `app/infrastructure/compute_job_factory.py`.
- Consumes: `RestApiComputeAdapter` from Task 1.

- [ ] **Step 1: Write failing test for factory**

```python
# tests/unit/infrastructure/test_duckdb_compute_adapter.py  (append at bottom)

def test_factory_returns_rest_api_adapter_for_rest_api_engine() -> None:
    """get_compute_adapter('rest_api') must return RestApiComputeAdapter."""
    from app.infrastructure.adapters.compute.rest_api_compute_adapter import RestApiComputeAdapter
    from app.infrastructure.compute_job_factory import get_compute_adapter

    adapter = get_compute_adapter("rest_api")
    assert isinstance(adapter, RestApiComputeAdapter)
```

- [ ] **Step 2: Run test to verify it fails**
```bash
uv run pytest tests/unit/infrastructure/test_duckdb_compute_adapter.py::test_factory_returns_rest_api_adapter_for_rest_api_engine -v
```
Expected: FAIL

- [ ] **Step 3: Run test after Task 1 implementation to verify it passes**
```bash
uv run pytest tests/unit/infrastructure/test_duckdb_compute_adapter.py -v
```
Expected: All tests PASS.

- [ ] **Step 4: Commit**
```bash
git add tests/unit/infrastructure/test_duckdb_compute_adapter.py
git commit -m "test: add factory test for rest_api engine in compute_job_factory"
```

---

### Task 3: HTTP Client, Authentication & Pagination Loop

**Files:**
- Modify: `app/infrastructure/adapters/compute/rest_api_compute_adapter.py`
- Modify: `tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py`

**Interfaces:**
- Consumes: `RestApiComputeAdapter._extract_async` stub from Task 1.
- Produces: `_extract_async` performing authenticated, paginated HTTP fetching with envelope unwrapping. `_build_auth_headers(auth_type, creds) -> dict[str, str]`. `_fetch_page(client, path, params) -> list[dict]` with tenacity retry.

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py (append)
import pytest
import respx
import httpx


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
        respx.get("http://api.test/items").respond(
            200, json={"data": [{"id": 1}, {"id": 2}], "next_cursor": "tok-2"}
        )
        respx.get("http://api.test/items", params={"cursor": "tok-2"}).respond(
            200, json={"data": [{"id": 3}], "next_cursor": None}
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
```

- [ ] **Step 2: Run tests to verify they fail**
```bash
uv run pytest tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py -k "pagination or single_page or bearer or envelope" -v
```
Expected: FAIL — `_extract_async` is a stub (no-op).

- [ ] **Step 3: Implement `_build_auth_headers`, `_fetch_page`, and `_extract_async` pagination loop**

```python
# app/infrastructure/adapters/compute/rest_api_compute_adapter.py
# Add these imports at the top:
import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

_WRAPPER_KEYS = ("data", "items", "results", "records", "content")
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class RestApiComputeAdapter:
    # ... existing methods unchanged ...

    def _build_auth_headers(self, auth_type: str, creds: dict[str, str]) -> dict[str, str]:
        """Build HTTP authentication headers from resolved credentials."""
        if auth_type == "bearer":
            return {"Authorization": f"Bearer {creds['token']}"}
        if auth_type == "api_key":
            return {"x-api-key": creds["api_key"]}
        if auth_type == "basic":
            import base64
            pair = base64.b64encode(
                f"{creds['username']}:{creds['password']}".encode()
            ).decode()
            return {"Authorization": f"Basic {pair}"}
        return {}

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(httpx.RequestError),
    )
    async def _fetch_page(
        self, client: httpx.AsyncClient, path: str, params: dict[str, Any]
    ) -> Any:
        """Fetch a single page, raising on 4xx/5xx. Tenacity retries on network errors."""
        resp = await client.get(path, params=params)
        if resp.status_code in _RETRYABLE_STATUS:
            resp.raise_for_status()  # will be caught by tenacity
        resp.raise_for_status()
        return resp.json()

    async def _extract_async(
        self,
        job_id: str,
        config: dict[str, Any],
        output_dir: Path,
    ) -> None:
        """Perform paginated HTTP extraction and stream-write to Parquet."""
        creds = await self._secret_manager.resolve(config["credential_ref"])
        headers = self._build_auth_headers(config.get("auth_type", ""), creds)

        pag_cfg: dict[str, Any] = config.get("pagination", {})
        strategy: str = pag_cfg.get("strategy", "none")
        page_size: int = pag_cfg.get("page_size", 100)

        output_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = output_dir / "data.parquet"

        import pyarrow as pa
        import pyarrow.parquet as pq
        import json

        buffer: list[dict[str, Any]] = []
        batch_size: int = config.get("batch_size", 5000)
        total_rows = 0
        pages_fetched = 0
        writer: pq.ParquetWriter | None = None

        async with httpx.AsyncClient(base_url=config["base_url"], headers=headers) as client:
            offset = 0
            cursor: str | None = None

            while True:
                params: dict[str, Any] = {}
                if strategy == "offset_limit":
                    params[pag_cfg.get("limit_param", "limit")] = page_size
                    params[pag_cfg.get("offset_param", "offset")] = offset
                elif strategy == "cursor" and cursor is not None:
                    params["cursor"] = cursor

                raw = await self._fetch_page(client, config["resource_path"], params)
                pages_fetched += 1

                # Unwrap envelope
                items: list[dict[str, Any]] = raw if isinstance(raw, list) else []
                if isinstance(raw, dict):
                    for key in _WRAPPER_KEYS:
                        if key in raw and isinstance(raw[key], list):
                            items = raw[key]
                            break

                buffer.extend(items)
                total_rows += len(items)

                # Flush batch
                if len(buffer) >= batch_size or strategy in ("none", "cursor") or len(items) < page_size:
                    if buffer:
                        table = pa.Table.from_pylist(buffer)
                        if writer is None:
                            writer = pq.ParquetWriter(parquet_path, table.schema)
                            schema_list = [
                                {"column": f.name, "type": str(f.type)} for f in table.schema
                            ]
                            (output_dir / "schema.json").write_text(
                                json.dumps(schema_list), encoding="utf-8"
                            )
                        writer.write_table(table)
                        buffer.clear()

                # Termination conditions
                if strategy == "none":
                    break
                if strategy == "offset_limit":
                    if len(items) < page_size:
                        break
                    offset += page_size
                elif strategy == "cursor":
                    cursor_key = pag_cfg.get("cursor_jsonpath", "next_cursor")
                    cursor = raw.get(cursor_key) if isinstance(raw, dict) else None
                    if not cursor:
                        break

        if writer:
            writer.close()

        import json as _json
        metrics = {
            "row_count": total_rows,
            "bytes_written": parquet_path.stat().st_size if parquet_path.exists() else 0,
            "pages_fetched": pages_fetched,
        }
        (output_dir / "metrics.json").write_text(_json.dumps(metrics), encoding="utf-8")
        logger.info("RestApi extraction complete: job=%s rows=%d", job_id, total_rows)
```

- [ ] **Step 4: Run all tests to verify they pass**
```bash
uv run pytest tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py -v
```
Expected: All tests PASS.

- [ ] **Step 5: Commit**
```bash
git add app/infrastructure/adapters/compute/rest_api_compute_adapter.py tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py
git commit -m "feat: implement http authentication, pagination loop and envelope unwrapping"
```

---

### Task 4: Parquet Output, Metrics & Schema Artifacts

**Files:**
- Modify: `tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py`

**Interfaces:**
- Consumes: `_extract_async` from Task 3 (Parquet writing already included in Task 3 implementation).
- Produces: Verified end-to-end output of `data.parquet`, `metrics.json`, `schema.json` from `_extract_async`.

Note: The Parquet writing logic is implemented in Task 3 alongside the pagination loop (they are tightly coupled). This task focuses exclusively on writing and running the assertions that verify the output artifacts are correct.

- [ ] **Step 1: Write failing test for full output artifacts**

```python
# tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py (append)
import json as _json
import pyarrow.parquet as pq
from pathlib import Path


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
            json=[{"id": 1, "name": "Widget", "price": 9.99}, {"id": 2, "name": "Gadget", "price": 19.99}],
        )
        await adapter._extract_async("job-out", config, output_dir)

    # data.parquet
    assert (output_dir / "data.parquet").exists()
    table = pq.read_table(output_dir / "data.parquet")
    assert table.num_rows == 2
    assert table.column_names == ["id", "name", "price"]

    # metrics.json
    assert (output_dir / "metrics.json").exists()
    metrics = _json.loads((output_dir / "metrics.json").read_text())
    assert metrics["row_count"] == 2
    assert metrics["bytes_written"] > 0
    assert metrics["pages_fetched"] == 1

    # schema.json
    assert (output_dir / "schema.json").exists()
    schema = _json.loads((output_dir / "schema.json").read_text())
    assert any(col["column"] == "id" for col in schema)
    assert any(col["column"] == "name" for col in schema)
```

- [ ] **Step 2: Run test to verify it fails**
```bash
uv run pytest tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py::test_extract_async_writes_parquet_metrics_schema -v
```
Expected: FAIL (stub `_extract_async` writes nothing).

- [ ] **Step 3: Verify test passes after Task 3 implementation**
```bash
uv run pytest tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py -v
```
Expected: All tests PASS.

- [ ] **Step 4: Run full suite to verify no regressions**
```bash
uv run pytest -m "not e2e"
```
Expected: All tests PASS.

- [ ] **Step 5: Commit**
```bash
git add tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py
git commit -m "test: add artifact output assertions for rest api compute adapter"
```
