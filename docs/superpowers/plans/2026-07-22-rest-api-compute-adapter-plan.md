# REST API Compute Adapter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `RestApiComputeAdapter` to extract paginated JSON payloads from REST APIs and write them efficiently into chunked Parquet files with automatic metrics and schema inference.

**Architecture:** A synchronous `ComputeJobAdapter` facade that launches an asynchronous extraction task via `ThreadPoolExecutor`. The worker thread resolves secrets, negotiates HTTP pagination, unwraps JSON envelopes, handles transient HTTP errors, and streams output via `pyarrow.parquet.ParquetWriter` for constant memory usage.

**Tech Stack:** Python 3.12, `httpx`, `pyarrow`, `tenacity` (for resilience).

## Global Constraints

- Must implement `ComputeJobAdapter` protocol.
- Must execute HTTP and Secret fetching strictly inside a thread pool to avoid blocking Airflow.
- Must not use `duckdb` for direct HTTP downloading (must use native `httpx` + `pyarrow`).
- Must handle 429 and 5xx errors gracefully using `tenacity`.

---

### Task 1: Component Scaffold & Factory Registration

**Files:**
- Create: `app/infrastructure/adapters/compute/rest_api_compute_adapter.py`
- Modify: `app/infrastructure/compute_job_factory.py`
- Create: `tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py`

**Interfaces:**
- Produces: `RestApiComputeAdapter` class implementing `submit_job`, `poll_job_status`, `cancel_job`.

- [ ] **Step 1: Write the failing test for factory registration**
```python
# tests/unit/infrastructure/test_compute_job_factory.py
# (assuming this file exists, just modify it or create a simple unit test for the factory)
from unittest.mock import MagicMock
from app.infrastructure.compute_job_factory import ComputeJobFactory
from app.infrastructure.adapters.compute.rest_api_compute_adapter import RestApiComputeAdapter

def test_factory_returns_rest_api_adapter():
    secret_manager = MagicMock()
    factory = ComputeJobFactory(secret_manager)
    adapter = factory.get_adapter("rest_api_ingestion", {})
    assert isinstance(adapter, RestApiComputeAdapter)
```

- [ ] **Step 2: Run test to verify it fails**
Run: `uv run pytest tests/unit/infrastructure/test_compute_job_factory.py -v`
Expected: FAIL (ModuleNotFoundError or AttributeError)

- [ ] **Step 3: Implement minimal RestApiComputeAdapter and register in factory**
```python
# app/infrastructure/adapters/compute/rest_api_compute_adapter.py
from typing import Any
from app.infrastructure.airflow_callbacks.compute_job_adapter import ComputeJobResult, JobStatus
from app.application.shared.secret_manager_port import SecretManagerPort

class RestApiComputeAdapter:
    def __init__(self, secret_manager: SecretManagerPort):
        self._secret_manager = secret_manager

    def submit_job(self, pipeline_id: str, pipeline_type: str, config: dict[str, Any]) -> str:
        return "fake-job-id"

    def poll_job_status(self, job_id: str) -> ComputeJobResult:
        return ComputeJobResult(job_id=job_id, status=JobStatus.RUNNING)

    def cancel_job(self, job_id: str) -> None:
        pass
```

```python
# app/infrastructure/compute_job_factory.py
# Add inside get_adapter:
from app.infrastructure.adapters.compute.rest_api_compute_adapter import RestApiComputeAdapter

class ComputeJobFactory:
    ...
    def get_adapter(self, pipeline_type: str, endpoint_config: dict[str, Any]) -> ComputeJobAdapter:
        if pipeline_type == "rest_api_ingestion":
            return RestApiComputeAdapter(secret_manager=self._secret_manager)
        ...
```

- [ ] **Step 4: Run test to verify it passes**
Run: `uv run pytest tests/unit/infrastructure/test_compute_job_factory.py -v`
Expected: PASS

- [ ] **Step 5: Commit**
```bash
git add app/infrastructure/adapters/compute/rest_api_compute_adapter.py app/infrastructure/compute_job_factory.py tests/unit/infrastructure/test_compute_job_factory.py
git commit -m "feat: scaffold RestApiComputeAdapter and factory registration"
```

---

### Task 2: Background Thread Execution & Resilience (Tenacity)

**Files:**
- Modify: `app/infrastructure/adapters/compute/rest_api_compute_adapter.py`
- Modify: `tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py`

**Interfaces:**
- Produces: `submit_job` spawns thread and tracks `JobState`.

- [ ] **Step 1: Write test for thread lifecycle**
```python
# tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py
from unittest.mock import MagicMock
from app.infrastructure.adapters.compute.rest_api_compute_adapter import RestApiComputeAdapter
from app.infrastructure.airflow_callbacks.compute_job_adapter import JobStatus
import time

def test_submit_and_poll_job():
    secret_manager = MagicMock()
    adapter = RestApiComputeAdapter(secret_manager)
    config = {"base_url": "http://fake", "resource_path": "/items", "credential_ref": "fake", "pagination": {"strategy": "none"}}
    job_id = adapter.submit_job("pipe1", "rest_api_ingestion", config)
    assert job_id is not None
    time.sleep(0.5)  # allow thread to fail or finish (will fail without real endpoint)
    result = adapter.poll_job_status(job_id)
    assert result.status in [JobStatus.SUCCESS, JobStatus.FAILED]
```

- [ ] **Step 2: Run test to verify it fails**
Run: `uv run pytest tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py -v`
Expected: FAIL (returns RUNNING statically)

- [ ] **Step 3: Implement ThreadPool execution and tenacity stub**
```python
# app/infrastructure/adapters/compute/rest_api_compute_adapter.py
import uuid
import asyncio
from pathlib import Path
from concurrent.futures import Future, ThreadPoolExecutor
from app.infrastructure.adapters.compute.job_state import JobState
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

class RestApiComputeAdapter:
    def __init__(self, secret_manager: SecretManagerPort, max_workers: int = 4):
        self._secret_manager = secret_manager
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._active_jobs: dict[str, JobState] = {}
        self._output_base_dir = Path("/tmp/rest_api_outputs")

    def submit_job(self, pipeline_id: str, pipeline_type: str, config: dict[str, Any]) -> str:
        job_id = str(uuid.uuid4())
        output_dir = self._output_base_dir / pipeline_id / job_id
        output_dir.mkdir(parents=True, exist_ok=True)
        future = self._executor.submit(self._run_extraction, job_id, config, output_dir)
        self._active_jobs[job_id] = JobState(job_id=job_id, status=JobStatus.RUNNING, future=future)
        return job_id

    def poll_job_status(self, job_id: str) -> ComputeJobResult:
        state = self._active_jobs.get(job_id)
        if not state:
            return ComputeJobResult(job_id=job_id, status=JobStatus.FAILED, error_message="Unknown job")
        if not state.future.done():
            return ComputeJobResult(job_id=job_id, status=JobStatus.RUNNING)
        exc = state.future.exception()
        if exc:
            return ComputeJobResult(job_id=job_id, status=JobStatus.FAILED, error_message=str(exc))
        return state.future.result()

    def cancel_job(self, job_id: str) -> None:
        if state := self._active_jobs.get(job_id):
            state.future.cancel()

    def _run_extraction(self, job_id: str, config: dict[str, Any], output_dir: Path) -> ComputeJobResult:
        asyncio.run(self._extract_async(job_id, config, output_dir))
        return ComputeJobResult(job_id=job_id, status=JobStatus.SUCCESS)

    async def _extract_async(self, job_id: str, config: dict[str, Any], output_dir: Path) -> None:
        pass # To be implemented in next task
```

- [ ] **Step 4: Run test to verify it passes**
Run: `uv run pytest tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py -v`
Expected: PASS

- [ ] **Step 5: Commit**
```bash
git add app/infrastructure/adapters/compute/rest_api_compute_adapter.py tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py
git commit -m "feat: implement background thread execution for RestApiComputeAdapter"
```

---

### Task 3: Secret Resolution, HTTP Client & Pagination Loop

**Files:**
- Modify: `app/infrastructure/adapters/compute/rest_api_compute_adapter.py`

**Interfaces:**
- Produces: `_extract_async` implementing httpx extraction with `offset_limit` and `cursor`.

- [ ] **Step 1: Write test for pagination and secret fetching**
```python
# tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py
import pytest
import respx
import httpx
from unittest.mock import AsyncMock

@pytest.mark.asyncio
async def test_extract_async_pagination():
    secret_manager = MagicMock()
    secret_manager.resolve = AsyncMock(return_value={"token": "abc"})
    adapter = RestApiComputeAdapter(secret_manager)
    config = {
        "base_url": "http://api.test",
        "resource_path": "/data",
        "credential_ref": "mock",
        "auth_type": "bearer",
        "pagination": {"strategy": "offset_limit", "page_size": 2, "limit_param": "limit", "offset_param": "offset"}
    }

    with respx.mock:
        respx.get("http://api.test/data?limit=2&offset=0").respond(200, json={"data": [{"id": 1}, {"id": 2}]})
        respx.get("http://api.test/data?limit=2&offset=2").respond(200, json={"data": [{"id": 3}]})
        await adapter._extract_async("job-1", config, Path("/tmp"))
        # We verify it doesn't crash. Parquet validation happens in Task 4.
```

- [ ] **Step 2: Run test to verify it fails**
Run: `uv run pytest tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py::test_extract_async_pagination -v`
Expected: FAIL/PASS depending on implementation (currently stubbed to pass doing nothing, but respx won't be called).

- [ ] **Step 3: Implement extraction loop with httpx**
```python
# app/infrastructure/adapters/compute/rest_api_compute_adapter.py
import httpx
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

class RestApiComputeAdapter:
    # ... existing code ...

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError))
    )
    async def _fetch_page(self, client: httpx.AsyncClient, url: str, params: dict) -> dict:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        return resp.json()

    async def _extract_async(self, job_id: str, config: dict[str, Any], output_dir: Path) -> None:
        creds = await self._secret_manager.resolve(config["credential_ref"])
        headers = {}
        if config.get("auth_type") == "bearer":
            headers["Authorization"] = f"Bearer {creds.get('token')}"
        elif config.get("auth_type") == "api_key":
            headers["x-api-key"] = creds.get("api_key")

        pag_cfg = config.get("pagination", {})
        strategy = pag_cfg.get("strategy", "none")
        page_size = pag_cfg.get("page_size", 100)

        async with httpx.AsyncClient(base_url=config["base_url"], headers=headers) as client:
            offset = 0
            while True:
                params = {}
                if strategy == "offset_limit":
                    params[pag_cfg.get("limit_param", "limit")] = page_size
                    params[pag_cfg.get("offset_param", "offset")] = offset

                data = await self._fetch_page(client, config["resource_path"], params)

                # Unpack envelope
                items = data
                if isinstance(data, dict):
                    for key in ["data", "items", "results", "records", "content"]:
                        if key in data and isinstance(data[key], list):
                            items = data[key]
                            break

                if not isinstance(items, list):
                    items = [items]

                # We will process items in Task 4

                if strategy == "none" or not items:
                    break
                if strategy == "offset_limit":
                    if len(items) < page_size:
                        break
                    offset += page_size
```

- [ ] **Step 4: Run test to verify it passes**
Run: `uv run pytest tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py::test_extract_async_pagination -v`
Expected: PASS

- [ ] **Step 5: Commit**
```bash
git add app/infrastructure/adapters/compute/rest_api_compute_adapter.py tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py
git commit -m "feat: implement httpx pagination and authentication loop"
```

---

### Task 4: Stream Writing PyArrow Parquet & Metrics

**Files:**
- Modify: `app/infrastructure/adapters/compute/rest_api_compute_adapter.py`

**Interfaces:**
- Produces: Fully functional `data.parquet`, `schema.json`, `metrics.json` outputs.

- [ ] **Step 1: Write test for Parquet and Metrics output**
```python
# tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py
import pyarrow.parquet as pq
import json

@pytest.mark.asyncio
async def test_parquet_and_metrics_output(tmp_path):
    secret_manager = MagicMock()
    secret_manager.resolve = AsyncMock(return_value={"token": "abc"})
    adapter = RestApiComputeAdapter(secret_manager)
    config = {
        "base_url": "http://api.test",
        "resource_path": "/data",
        "credential_ref": "mock",
        "pagination": {"strategy": "none"}
    }

    with respx.mock:
        respx.get("http://api.test/data").respond(200, json=[{"id": 1, "name": "A"}])
        await adapter._extract_async("job-1", config, tmp_path)

    assert (tmp_path / "data.parquet").exists()
    assert (tmp_path / "metrics.json").exists()
    assert (tmp_path / "schema.json").exists()

    table = pq.read_table(tmp_path / "data.parquet")
    assert table.num_rows == 1

    metrics = json.loads((tmp_path / "metrics.json").read_text())
    assert metrics["row_count"] == 1
```

- [ ] **Step 2: Run test to verify it fails**
Run: `uv run pytest tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py::test_parquet_and_metrics_output -v`
Expected: FAIL (No such file or directory)

- [ ] **Step 3: Implement PyArrow ParquetWriter and JSON dumps**
```python
# app/infrastructure/adapters/compute/rest_api_compute_adapter.py
import json
import pyarrow as pa
import pyarrow.parquet as pq

# Modify inside _extract_async:
    async def _extract_async(self, job_id: str, config: dict[str, Any], output_dir: Path) -> None:
        # [existing setup code]
        batch_size = config.get("batch_size", 5000)
        buffer = []
        total_rows = 0
        pages_fetched = 0

        writer = None
        parquet_path = output_dir / "data.parquet"

        async with httpx.AsyncClient(base_url=config["base_url"], headers=headers) as client:
            offset = 0
            while True:
                # [existing fetch code]
                data = await self._fetch_page(client, config["resource_path"], params)
                pages_fetched += 1

                # [existing unwrapping code -> items]

                buffer.extend(items)
                total_rows += len(items)

                if len(buffer) >= batch_size or not items or strategy == "none":
                    if buffer:
                        table = pa.Table.from_pylist(buffer)
                        if writer is None:
                            writer = pq.ParquetWriter(parquet_path, table.schema)
                            schema_list = [{"column": field.name, "type": str(field.type)} for field in table.schema]
                            (output_dir / "schema.json").write_text(json.dumps(schema_list))
                        writer.write_table(table)
                        buffer.clear()

                if strategy == "none" or not items:
                    break
                # [offset logic]

        if writer:
            writer.close()

        metrics = {
            "row_count": total_rows,
            "bytes_written": parquet_path.stat().st_size if parquet_path.exists() else 0,
            "pages_fetched": pages_fetched
        }
        (output_dir / "metrics.json").write_text(json.dumps(metrics))

    # Also update _run_extraction to return proper paths:
    def _run_extraction(self, job_id: str, config: dict[str, Any], output_dir: Path) -> ComputeJobResult:
        asyncio.run(self._extract_async(job_id, config, output_dir))
        return ComputeJobResult(
            job_id=job_id,
            status=JobStatus.SUCCESS,
            output_path=str(output_dir / "data.parquet"),
            metrics_path=str(output_dir / "metrics.json"),
            schema_path=str(output_dir / "schema.json"),
        )
```

- [ ] **Step 4: Run test to verify it passes**
Run: `uv run pytest tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py -v`
Expected: PASS

- [ ] **Step 5: Commit**
```bash
git add app/infrastructure/adapters/compute/rest_api_compute_adapter.py tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py
git commit -m "feat: implement stream writing pyarrow parquet and metrics generation"
```
