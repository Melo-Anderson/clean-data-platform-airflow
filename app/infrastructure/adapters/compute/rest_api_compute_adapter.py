from __future__ import annotations

import asyncio
import json
import logging
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
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
            return {"x-api-key": creds["api_key"]}
        if auth_type == "basic":
            import base64

            pair = base64.b64encode(f"{creds['username']}:{creds['password']}".encode()).decode()
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
        creds = await self._secret_manager.resolve(config["credential_ref"])
        headers = self._build_auth_headers(config.get("auth_type", ""), creds)

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

        async with httpx.AsyncClient(base_url=config["base_url"], headers=headers) as client:
            offset = 0
            page_num = pag_cfg.get("page_start", 1)
            cursor: str | None = None

            while True:
                params: dict[str, Any] = {}
                if strategy == "offset_limit":
                    params[pag_cfg.get("limit_param", "limit")] = page_size
                    params[pag_cfg.get("offset_param", "offset")] = offset
                elif strategy == "page_number":
                    params[pag_cfg.get("limit_param", "limit")] = page_size
                    params[pag_cfg.get("page_param", "page")] = page_num
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

        metrics = {
            "row_count": total_rows,
            "bytes_written": parquet_path.stat().st_size if parquet_path.exists() else 0,
            "pages_fetched": pages_fetched,
        }
        (output_dir / "metrics.json").write_text(json.dumps(metrics), encoding="utf-8")
        logger.info("RestApi extraction complete: job=%s rows=%d", job_id, total_rows)
