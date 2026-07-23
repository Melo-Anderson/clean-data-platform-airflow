# REST API Compute Adapter Design Specification

## Overview

This specification details the design for `RestApiComputeAdapter`, a dedicated compute engine adapter for extracting data from REST API endpoints into Parquet datasets with automatic metrics and schema inference.

It implements the platform's `ComputeJobAdapter` protocol (`submit_job`, `poll_job_status`, `cancel_job`), isolating all HTTP extraction, pagination, credential resolution, and PyArrow stream-writing logic within the infrastructure layer. All compute execution is encapsulated and never leaks into the DAG layer.

---

## Component Architecture

- **Class:** `RestApiComputeAdapter`
- **Module:** `app/infrastructure/adapters/compute/rest_api_compute_adapter.py`
- **Protocol Implemented:** `ComputeJobAdapter` (`app/infrastructure/airflow_callbacks/compute_job_adapter.py`)
- **Execution Model:** Asynchronous job submission using a background `ThreadPoolExecutor` — identical in pattern to `DuckDbComputeAdapter`. Secret resolution is performed inside the execution thread via `asyncio.run(self._secret_manager.resolve(...))` to guarantee event loop isolation from the Airflow worker thread.
- **Factory:** Registered via `get_compute_adapter("rest_api")` in `app/infrastructure/compute_job_factory.py`.

```
+-------------------------------------------------------------------------+
|                              Airflow DAG                                |
|  (@task.sensor monitor_compute_job -> ComputeJobAdapter interface)      |
+-------------------------------------------------------------------------+
                                     |
                                     v
+-------------------------------------------------------------------------+
|                        RestApiComputeAdapter                            |
|                                                                         |
|  submit_job()       -> ThreadPoolExecutor.submit(_run_extraction)       |
|  poll_job_status()  -> Future.done() / .exception() / .result()        |
|  cancel_job()       -> Future.cancel()                                  |
+-------------------------------------------------------------------------+
                                     |
                                     v
+-------------------------------------------------------------------------+
|                    _run_extraction (Background Thread)                  |
|                                                                         |
|  1. asyncio.run(_extract_async(...))                                    |
|     a. Secret resolution via SecretManagerPort                          |
|     b. Build httpx.AsyncClient with auth headers                        |
|     c. Paginate: offset_limit | cursor | none                           |
|     d. Unwrap envelopes: data, items, results, records, content         |
|     e. Chunked writes via pyarrow.parquet.ParquetWriter                 |
|     f. Write data.parquet, metrics.json, schema.json                    |
|  2. Return ComputeJobResult(output_path, metrics_path, schema_path)    |
+-------------------------------------------------------------------------+
```

---

## Configuration Schema

Configuration is declared in pipeline YAML files by Analytics Engineers. The orchestrator resolves the `endpoint_id`, merges credentials metadata, and passes the resulting config dict to `submit_job`:

```yaml
# pipeline.yaml (Analytics Engineer declares this)
id: pipeline_extract_products
type: rest_api_ingestion
endpoint_id: ep_mock_store
resource_path: /api/v1/products
pagination:
  strategy: offset_limit    # offset_limit | cursor | none
  page_size: 100
  limit_param: limit
  offset_param: offset
  cursor_jsonpath: pagination.next_cursor   # only used when strategy == cursor
batch_size: 5000            # flush threshold for PyArrow ParquetWriter
```

The orchestrator passes this resolved dict to `submit_job`:

```python
{
    "base_url": "http://mock-store:8081",
    "resource_path": "/api/v1/products",
    "credential_ref": "vault/api/mock-store",
    "auth_type": "bearer",          # "bearer" | "api_key" | "basic" | ""
    "pagination": {
        "strategy": "offset_limit",
        "page_size": 100,
        "limit_param": "limit",
        "offset_param": "offset",
        "cursor_jsonpath": "pagination.next_cursor",  # required if strategy == "cursor"
    },
    "batch_size": 5000,
}
```

---

## Pagination & Extraction Engine

### 1. Authentication

Credentials are resolved via `SecretManagerPort.resolve(credential_ref)` inside the background thread. Auth header is injected into `httpx.AsyncClient`:

| `auth_type` | Header injected |
|---|---|
| `bearer` | `Authorization: Bearer <token>` |
| `api_key` | `x-api-key: <api_key>` |
| `basic` | `Authorization: Basic <base64(user:password)>` |
| `""` (empty) | No auth header |

### 2. Pagination Strategies

| Strategy | Termination condition |
|---|---|
| `offset_limit` | Page returns 0 items or fewer items than `page_size` |
| `cursor` | Cursor field (`cursor_jsonpath`) resolves to `None` or empty string |
| `none` | Single HTTP GET; terminates immediately after first response |

### 3. Envelope Unwrapping

Automatically detects and unwraps standard envelope keys in order: `data`, `items`, `results`, `records`, `content`. If none match, treats the full response as the item list.

### 4. Memory-Safe Chunked Writing

- Maintains constant RAM footprint: $O(\text{batch\_size})$ records max in memory at any time.
- Records accumulate up to `batch_size` items (default: 5,000).
- Each batch is converted to `pyarrow.Table.from_pylist(batch)` and appended to `data.parquet` via a single open `pyarrow.parquet.ParquetWriter` instance (schema is inferred from the first batch and fixed for all subsequent batches).
- On termination, `ParquetWriter.close()` is called to flush and finalize.

### 5. Resilience & Retry

Uses `tenacity` (already a project dependency) to handle transient HTTP failures:

- **Retry conditions:** `httpx.RequestError` (network), and `httpx.HTTPStatusError` for 429, 500, 502, 503, 504.
- **Strategy:** Exponential backoff, min 2s, max 10s, up to 3 retries.
- **HTTP 429:** Respects `Retry-After` response header when present.
- Non-retryable errors (4xx except 429) raise immediately.

---

## Output Artifacts & Status Contract

All artifacts are persisted to the isolated job directory:
`/tmp/rest_api_outputs/<pipeline_id>/<job_id>/`

### `data.parquet`
Parquet file containing all extracted records with native PyArrow-inferred types.

### `metrics.json`
```json
{
  "row_count": 15420,
  "bytes_written": 2450128,
  "pages_fetched": 155,
  "duration_seconds": 4.12
}
```

### `schema.json`
```json
[
  {"column": "id", "type": "int64"},
  {"column": "name", "type": "string"},
  {"column": "price", "type": "double"}
]
```

### `ComputeJobResult` (from `poll_job_status`)
```python
ComputeJobResult(
    job_id=job_id,
    status=JobStatus.SUCCESS,
    output_path="/tmp/rest_api_outputs/<pipeline_id>/<job_id>/data.parquet",
    metrics_path="/tmp/rest_api_outputs/<pipeline_id>/<job_id>/metrics.json",
    schema_path="/tmp/rest_api_outputs/<pipeline_id>/<job_id>/schema.json",
)
```

---

## Factory Registration

The function `get_compute_adapter` in `app/infrastructure/compute_job_factory.py` is extended to support engine key `"rest_api"`:

```python
def get_compute_adapter(engine: str) -> ComputeJobAdapter:
    if engine == "duckdb":
        ...
    if engine == "rest_api":
        from app.infrastructure.adapters.compute.rest_api_compute_adapter import RestApiComputeAdapter
        from app.infrastructure.adapters.secrets.secret_manager_factory import get_secret_manager
        return RestApiComputeAdapter(secret_manager=get_secret_manager(get_settings()))
    return DummyComputeAdapter()
```

---

## Verification Plan

### Unit Tests (no I/O, no Docker required)
`tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py`

- Named mock `MockSecretManager` (no `MagicMock`), following project conventions.
- Manipulate `Future` directly (no `time.sleep`) to test `poll_job_status` state transitions.
- `respx.mock` for httpx HTTP interception.

Test cases:
- `test_submit_returns_uuid_and_registers_job` — submit_job returns UUID, registers in `_active_jobs`
- `test_poll_returns_running_while_future_pending` — future unresolved → RUNNING
- `test_poll_returns_success_after_future_completes` — future resolved → SUCCESS with paths
- `test_poll_returns_failed_when_future_raises` — future exception → FAILED with error message
- `test_poll_returns_failed_for_unknown_job_id` — unknown job_id → FAILED
- `test_cancel_job_calls_future_cancel` — cancel_job cancels the future
- `test_extract_async_offset_limit_pagination` — 2 pages via respx, verifies termination
- `test_extract_async_cursor_pagination` — cursor strategy terminates on null cursor
- `test_extract_async_single_page` — strategy "none" makes 1 request
- `test_extract_async_unwraps_envelope` — `{"data": [...]}` envelope is unwrapped
- `test_extract_async_writes_parquet_metrics_schema` — all 3 output files created with correct content
- `test_factory_returns_rest_api_adapter_for_rest_api_engine`

### Static Verification
```bash
uv run pytest tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py -v
uv run pytest -m "not e2e"
```
