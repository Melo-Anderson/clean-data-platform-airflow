# REST API Compute Adapter Design Specification

## Overview

This specification details the design for `RestApiComputeAdapter`, a dedicated compute engine adapter for extracting data from REST API endpoints into local Parquet datasets, schema metadata, and execution metrics.

It implements the platform's `ComputeJobAdapter` protocol (`submit_job`, `poll_job_status`, `cancel_job`), isolating all HTTP extraction, pagination, credential resolution, and PyArrow stream-writing logic within the infrastructure layer.

---

## Component Architecture

- **Class:** `RestApiComputeAdapter`
- **Module:** `app/infrastructure/adapters/compute/rest_api_compute_adapter.py`
- **Protocol Implemented:** `ComputeJobAdapter` (`app/infrastructure/airflow_callbacks/compute_job_adapter.py`)
- **Execution Model:** Asynchronous job submission using a background `ThreadPoolExecutor`. Secret resolution is performed inside the execution thread via `asyncio.run(self._secret_manager.resolve(...))` to guarantee event loop isolation.

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
|  1. submit_job() -----> ThreadPoolExecutor (Background Thread)         |
|  2. poll_job_status() -> Checks Future & returns ComputeJobResult       |
|  3. cancel_job() ------> Cancels background Future                      |
+-------------------------------------------------------------------------+
                                     |
                                     v
+-------------------------------------------------------------------------+
|                         Background Worker Thread                        |
|                                                                         |
|  1. Secret Resolution via SecretManagerPort                             |
|  2. HTTP Pagination (httpx.Client: Offset/Limit, Cursor, Single Page)   |
|  3. Envelope Unwrapping ('data', 'items', 'results', 'records')         |
|  4. Chunked Stream Writing (pyarrow.parquet.ParquetWriter)               |
|  5. Write data.parquet, metrics.json, and schema.json to output dir     |
+-------------------------------------------------------------------------+
```

---

## Configuration Schema

Jobs are declared in pipeline definition YAML files by Analytics Engineers. The pipeline orchestrator passes this configuration dictionary to `submit_job`:

```python
{
    "base_url": "http://mock-store:8081",
    "resource_path": "/api/v1/products",
    "credential_ref": "vault/api/mock-store",
    "auth_type": "bearer",  # "bearer" | "api_key" | "basic" | ""
    "pagination": {
        "strategy": "offset_limit",  # "offset_limit" | "cursor" | "none"
        "page_size": 100,
        "limit_param": "limit",
        "offset_param": "offset",  # or "page"
        "cursor_jsonpath": "pagination.next_cursor",
    },
    "batch_size": 5000,  # Flush threshold for memory safety
}
```

---

## Pagination & Extraction Engine

1. **Authentication:**
   - Resolves credentials via `SecretManagerPort`.
   - Supports `bearer` (`Authorization: Bearer <token>`), `api_key` (`x-api-key: <token>`), `basic` (`Authorization: Basic <base64>`), and unauthenticated requests (`""`).

2. **Pagination Strategies:**
   - **`offset_limit`:** Increments `offset += page_size` (or `page += 1`) on each iteration. Terminates when a page returns 0 items or fewer items than `page_size`.
   - **`cursor`:** Extracts `next_cursor` from the payload (or pagination envelope). Terminates when the cursor is `None` or empty.
   - **`none`:** Executes a single HTTP GET request for unpaginated or small endpoints.

3. **Envelope Unwrapping:**
   - Automatically detects top-level wrapper keys (`data`, `items`, `results`, `records`, `content`) if items are nested inside an envelope dict.

4. **Memory-Safe Chunked Writing:**
   - Maintains a constant $O(\text{batch\_size})$ RAM footprint.
   - Records are accumulated up to `batch_size` items (default: 5,000).
   - Each batch is converted into a `pyarrow.Table` and appended directly to `data.parquet` via `pyarrow.parquet.ParquetWriter`.

5. **Resilience & Retry Mechanism:**
   - Implement exponential backoff retries (e.g., using `tenacity` or `httpx` async hooks) for transient HTTP errors.
   - Specifically handle HTTP 429 (Too Many Requests) by respecting the `Retry-After` header if present.
   - Handle HTTP 500, 502, 503, and 504 with a maximum of 3 to 5 retries to ensure pipeline robustness.

---

## Output Artifacts & Status Contract

All artifacts are persisted to the isolated directory:
`/tmp/rest_api_outputs/<pipeline_id>/<job_id>/`

### 1. `data.parquet`
Target Parquet file containing all extracted records with native PyArrow types.

### 2. `metrics.json`
```json
{
  "row_count": 15420,
  "bytes_written": 2450128,
  "http_requests_total": 155,
  "pages_fetched": 155,
  "duration_seconds": 4.12
}
```

### 3. `schema.json`
```json
[
  {"column": "id", "type": "int64"},
  {"column": "name", "type": "string"},
  {"column": "price", "type": "double"}
]
```

### 4. `ComputeJobResult`
`poll_job_status(job_id)` returns:
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

## Factory & Registration

- **Factory:** `ComputeJobFactory` (`app/infrastructure/compute_job_factory.py`)
- Supports instantiating `RestApiComputeAdapter` when `pipeline_type == "rest_api_ingestion"`.

---

## Verification Plan

### Automated Unit Tests
- `tests/unit/infrastructure/adapters/compute/test_rest_api_compute_adapter.py`
  - Authentication headers building (`bearer`, `api_key`, `basic`, empty).
  - Offset/Limit pagination extraction.
  - Cursor pagination extraction.
  - Single page extraction.
  - Envelope unwrapping.
  - Memory-safe chunked Parquet writing (`data.parquet`, `metrics.json`, `schema.json`).
  - Job cancellation.
- `tests/unit/infrastructure/test_compute_job_factory.py`
  - Instantiation of `RestApiComputeAdapter` for `rest_api_ingestion`.

### Static Verification
- `uv run pytest -m "not e2e"`
