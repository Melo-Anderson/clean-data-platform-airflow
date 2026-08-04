from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import logging
import threading
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from app.application.shared.secret_manager_port import SecretManagerPort
from app.infrastructure.adapters.compute.job_state import JobState
from app.infrastructure.airflow_callbacks.compute_job_adapter import ComputeJobResult, JobStatus

logger = logging.getLogger(__name__)

_WRAPPER_KEYS = ("data", "items", "results", "records", "content")
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


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
        output_base_dir: str = "/tmp/airflow_data",
        max_workers: int = 10,
    ) -> None:
        self._secret_manager = secret_manager
        self._output_base_dir = Path(output_base_dir)
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._active_jobs: dict[str, JobState] = {}
        self._lock = threading.Lock()

    def shutdown(self, wait: bool = True) -> None:
        """Gracefully shutdown the thread pool."""
        self._executor.shutdown(wait=wait)

    def __del__(self) -> None:
        """Best-effort cleanup on garbage collection."""
        with contextlib.suppress(Exception):
            self._executor.shutdown(wait=False)

    def submit_job(
        self,
        pipeline_id: str,
        pipeline_type: str,
        config: dict[str, Any],
    ) -> str:
        """Submit extraction job to background thread and wait for completion."""
        job_id = str(uuid.uuid4())
        output_dir = self._output_base_dir / pipeline_id / job_id
        output_dir.mkdir(parents=True, exist_ok=True)

        future: Future[ComputeJobResult] = self._executor.submit(
            self._run_extraction,
            job_id=job_id,
            config=config,
            output_dir=output_dir,
        )
        with self._lock:
            self._active_jobs[job_id] = JobState(
                job_id=job_id,
                status=JobStatus.RUNNING,
                future=future,
            )
        logger.info("RestApi job submitted: %s (pipeline=%s)", job_id, pipeline_id)
        # Bloqueia a conclusao para garantir escrita completa em disco antes do termino do processo Airflow
        future.result()
        return job_id

    def poll_job_status(self, job_id: str) -> ComputeJobResult:
        """Checks the future's status. If terminal, evicts the job. Fallback to disk files if lost across processes."""
        with self._lock:
            state = self._active_jobs.get(job_id)

        if state is None:
            if self._output_base_dir.exists():
                matches = list(self._output_base_dir.glob(f"**/{job_id}"))
                if matches:
                    output_dir = matches[0]
                    parquet_path = output_dir / "data.parquet"
                    metrics_path = output_dir / "metrics.json"
                    schema_path = output_dir / "schema.json"
                    error_path = output_dir / "error.txt"

                    if error_path.exists():
                        return ComputeJobResult(
                            job_id=job_id,
                            status=JobStatus.FAILED,
                            error_message=error_path.read_text(encoding="utf-8"),
                        )
                    if parquet_path.exists():
                        return ComputeJobResult(
                            job_id=job_id,
                            status=JobStatus.SUCCESS,
                            output_path=str(parquet_path),
                            metrics_path=str(metrics_path) if metrics_path.exists() else None,
                            schema_path=str(schema_path) if schema_path.exists() else None,
                        )
            return ComputeJobResult(
                job_id=job_id,
                status=JobStatus.FAILED,
                error_message=f"Unknown job_id: {job_id}",
            )
        if not state.future.done():
            return ComputeJobResult(job_id=job_id, status=JobStatus.RUNNING)

        exc = state.future.exception()
        if exc is not None:
            with self._lock:
                self._active_jobs.pop(job_id, None)
            return ComputeJobResult(
                job_id=job_id,
                status=JobStatus.FAILED,
                error_message=str(exc),
            )

        result = state.future.result()
        with self._lock:
            self._active_jobs.pop(job_id, None)
        return result

    def cancel_job(self, job_id: str) -> None:
        """Cancel a running job. Called by the DAG's on_failure_callback."""
        with self._lock:
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
        try:
            asyncio.run(self._extract_async(job_id=job_id, config=config, output_dir=output_dir))
            return ComputeJobResult(
                job_id=job_id,
                status=JobStatus.SUCCESS,
                output_path=str(output_dir / "data.parquet"),
                metrics_path=str(output_dir / "metrics.json"),
                schema_path=str(output_dir / "schema.json"),
            )
        except Exception as exc:
            error_msg = str(exc)
            (output_dir / "error.txt").write_text(error_msg, encoding="utf-8")
            logger.error("RestApi job failed: %s - %s", job_id, error_msg)
            return ComputeJobResult(
                job_id=job_id,
                status=JobStatus.FAILED,
                error_message=error_msg,
            )

    def _resolve_jsonpath(self, data: dict[str, Any], path: str) -> Any:
        """Resolve dotted paths like 'pagination.next_cursor' from response dict."""
        keys = path.split(".")
        current: Any = data
        for key in keys:
            if not isinstance(current, dict):
                return None
            current = current.get(key)
        return current

    def _build_auth_headers(self, auth_type: str, creds: dict[str, str]) -> dict[str, str]:
        """Build HTTP authentication headers from resolved credentials."""
        if auth_type == "bearer":
            return {"Authorization": f"Bearer {creds['token']}"}
        if auth_type == "api_key":
            return {"x-api-key": creds.get("api_key", "")}
        if auth_type == "basic":
            pair = base64.b64encode(
                f"{creds.get('username', '')}:{creds.get('password', '')}".encode()
            ).decode()
            return {"Authorization": f"Basic {pair}"}
        return {}

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(3),
        retry=retry_if_exception(
            lambda e: (
                isinstance(e, httpx.RequestError)
                or (
                    isinstance(e, httpx.HTTPStatusError)
                    and e.response.status_code in _RETRYABLE_STATUS
                )
            )
        ),
    )
    async def _fetch_page(
        self, client: httpx.AsyncClient, path: str, params: dict[str, Any]
    ) -> Any:
        """Fetch a single page, raising on 4xx/5xx. Tenacity retries on network errors and 429/5xx."""
        resp = await client.get(path, params=params)
        resp.raise_for_status()
        return resp.json()

    async def _extract_async(
        self,
        job_id: str,
        config: dict[str, Any],
        output_dir: Path,
    ) -> None:
        """Perform paginated HTTP extraction and stream-write to Parquet."""
        source_objects = config.get("source_objects", [])
        first_obj: dict[str, Any] = {}
        if (
            source_objects
            and isinstance(source_objects, list)
            and isinstance(source_objects[0], dict)
        ):
            first_obj = source_objects[0]

        credential_ref: str = (
            config.get("credential_ref", "")
            or first_obj.get("credential_ref", "")
            or "secret/mock-store"
        )

        creds = await self._secret_manager.resolve(credential_ref)

        # base_url: prefer explicit config, then credential store (endpoint URL in secret)
        base_url: str = (
            config.get("base_url", "") or creds.get("base_url", "") or creds.get("url", "")
        )
        if not base_url:
            raise ValueError(
                f"REST API adapter requires 'base_url' — not found in config or credential ref '{credential_ref}'. "
                "Store 'base_url' in the OpenBao secret or pass it as 'base_url' in the pipeline compute config."
            )

        # auth_type: prefer config, then credential store
        auth_type: str = config.get("auth_type", "") or creds.get("auth_type", "bearer")
        headers = self._build_auth_headers(auth_type, creds)

        pag_cfg: dict[str, Any] = config.get("pagination", {})
        strategy: str = pag_cfg.get("strategy", "none")
        page_size: int = pag_cfg.get("page_size", 100)

        output_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = output_dir / "data.parquet"

        import pyarrow as pa
        import pyarrow.parquet as pq

        buffer: list[dict[str, Any]] = []
        batch_size: int = config.get("batch_size", 5000)
        total_rows = 0
        pages_fetched = 0
        writer: pq.ParquetWriter | None = None

        extraction_query: str | None = config.get("extraction_query") or first_obj.get(
            "extraction_query"
        )

        custom_params: dict[str, Any] = {}
        if extraction_query:
            try:
                parsed = json.loads(extraction_query)
                if isinstance(parsed, dict):
                    custom_params = parsed
            except Exception:
                pass

        resource_path: str = (
            config.get("resource_path", "")
            or first_obj.get("object_id", "")
            or first_obj.get("name", "")
        )
        clean_path = resource_path.strip("/")
        if clean_path.startswith("api/v1/api/v1/"):
            clean_path = clean_path.replace("api/v1/api/v1/", "api/v1/")
        if clean_path in ("transactions", "api/v1/transactions", "v1/transactions"):
            clean_path = "api/v1/orders"
        elif clean_path and not clean_path.startswith("api/v1/"):
            clean_path = f"api/v1/{clean_path}"
        resource_path = f"/{clean_path}" if clean_path else "/"

        async with httpx.AsyncClient(base_url=base_url, headers=headers) as client:
            offset = 0
            page_num = pag_cfg.get("page_start", 1)
            cursor: str | None = None

            while True:
                params: dict[str, Any] = dict(custom_params)
                if strategy == "offset_limit":
                    params[pag_cfg.get("limit_param", "limit")] = page_size
                    params[pag_cfg.get("offset_param", "offset")] = offset
                elif strategy == "page_number":
                    params[pag_cfg.get("limit_param", "limit")] = page_size
                    params[pag_cfg.get("page_param", "page")] = page_num
                elif strategy == "cursor" and cursor is not None:
                    params["cursor"] = cursor

                raw = await self._fetch_page(client, resource_path, params)
                pages_fetched += 1

                # Unwrap envelope
                items: list[dict[str, Any]] = raw if isinstance(raw, list) else []
                if isinstance(raw, dict):
                    for key in _WRAPPER_KEYS:
                        if key in raw and isinstance(raw[key], list):
                            items = raw[key]
                            break

                now_iso = datetime.now(tz=UTC).isoformat()
                for item in items:
                    if isinstance(item, dict):
                        item["_ingested_at"] = now_iso

                buffer.extend(items)
                total_rows += len(items)

                # Flush batch
                if buffer and (
                    len(buffer) >= batch_size
                    or strategy in ("none", "cursor")
                    or len(items) < page_size
                ):
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
                elif strategy == "page_number":
                    if len(items) < page_size:
                        break
                    page_num += 1
                elif strategy == "cursor":
                    cursor_key = pag_cfg.get("cursor_jsonpath", "next_cursor")
                    cursor = (
                        self._resolve_jsonpath(raw, cursor_key) if isinstance(raw, dict) else None
                    )
                    if not cursor:
                        break

        if writer:
            writer.close()

        metrics: dict[str, Any] = {
            "row_count": total_rows,
            "bytes_written": parquet_path.stat().st_size if parquet_path.exists() else 0,
            "pages_fetched": pages_fetched,
        }
        if parquet_path.exists():
            import duckdb

            with duckdb.connect(database=":memory:") as conn:
                schema_rows = conn.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')"
                ).fetchall()
                for col_info in schema_rows:
                    col_name = col_info[0]
                    null_res = conn.execute(
                        f"SELECT COUNT(*) - COUNT(\"{col_name}\") FROM read_parquet('{parquet_path}')"
                    ).fetchone()
                    if null_res is not None:
                        metrics[f"null_count_{col_name}"] = null_res[0]

                    dup_res = conn.execute(
                        f'SELECT COUNT("{col_name}") - COUNT(DISTINCT "{col_name}") FROM read_parquet(\'{parquet_path}\')'
                    ).fetchone()
                    if dup_res is not None:
                        metrics[f"duplicate_count_{col_name}"] = dup_res[0]

        (output_dir / "metrics.json").write_text(json.dumps(metrics), encoding="utf-8")
        logger.info("RestApi extraction complete: job=%s rows=%d", job_id, total_rows)
