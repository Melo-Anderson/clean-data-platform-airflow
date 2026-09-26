from __future__ import annotations

import asyncio
import json
import logging
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from motor.motor_asyncio import AsyncIOMotorClient

from app.application.shared.ports import SecretManagerPort
from app.infrastructure.adapters.compute.job_state import JobState
from app.infrastructure.adapters.compute.rest_api_helpers import calculate_parquet_metrics
from app.infrastructure.adapters.compute.source_dtos import (
    DatabaseConnectionDTO,
    PipelineExecutionTargetDTO,
)
from app.infrastructure.airflow_callbacks.compute_job_adapter import ComputeJobResult, JobStatus
from app.infrastructure.discovery.connection_url_builder import build_connection_url

logger = logging.getLogger(__name__)


class DuckDbComputeAdapter:
    """
    Motor de compute local usando DuckDB em thread de background.

    Implementa o mesmo contrato síncrono do ComputeJobAdapter (submit → poll → cancel)
    para que as tasks da DAG funcionem sem modificação.

    Credenciais são resolvidas dentro da thread via asyncio.run() porque:
    - submit_job é síncrono (Protocol não permite async)
    - Threads do Airflow worker não têm event loop ativa
    - asyncio.run() cria uma event loop isolada por chamada
    """

    def __init__(
        self,
        secret_manager: SecretManagerPort,
        output_base_dir: str | Path,
        max_workers: int = 4,
        default_credential_ref: str = "",
    ) -> None:
        self._secret_manager = secret_manager
        self._output_base_dir = Path(output_base_dir)
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._active_jobs: dict[str, JobState] = {}
        self._default_credential_ref = default_credential_ref

    def submit_job(
        self,
        pipeline_id: str,
        pipeline_type: str,
        config: dict[str, Any],
    ) -> str:
        """
        Submete a extração DuckDB em background thread.
        """
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

        logger.info("DuckDB job submitted: %s", job_id)
        return job_id

    def poll_job_status(self, job_id: str) -> ComputeJobResult:
        if job_id in self._active_jobs:
            state = self._active_jobs[job_id]
            if not state.future.done():
                return ComputeJobResult(job_id=job_id, status=JobStatus.RUNNING)

            try:
                return state.future.result()
            except Exception as exc:
                logger.error("Job %s falhou com excecao: %s", job_id, exc)
                return ComputeJobResult(
                    job_id=job_id,
                    status=JobStatus.FAILED,
                    error_message=str(exc),
                )

        # Fallback to disk inspection across Airflow worker processes
        matches = list(self._output_base_dir.glob(f"**/{job_id}"))
        if not matches:
            return ComputeJobResult(
                job_id=job_id,
                status=JobStatus.FAILED,
                error_message=f"job_id desconhecido: {job_id}",
            )

        job_dir = matches[0]
        error_file = job_dir / "error.txt"
        if error_file.exists():
            return ComputeJobResult(
                job_id=job_id,
                status=JobStatus.FAILED,
                error_message=error_file.read_text("utf-8"),
            )

        parquet_files = sorted(job_dir.glob("*.parquet*"))
        if parquet_files:
            metrics_file = job_dir / "metrics.json"
            schema_file = job_dir / "schema.json"
            return ComputeJobResult(
                job_id=job_id,
                status=JobStatus.SUCCESS,
                output_path=str(parquet_files[0]),
                metrics_path=str(metrics_file) if metrics_file.exists() else None,
                schema_path=str(schema_file) if schema_file.exists() else None,
            )

        return ComputeJobResult(job_id=job_id, status=JobStatus.RUNNING)

    def cancel_job(self, job_id: str) -> None:
        if job_id in self._active_jobs:
            self._active_jobs[job_id].future.cancel()
            self._active_jobs[job_id].status = JobStatus.CANCELLED
            logger.info("DuckDB job cancelled: %s", job_id)

    def _run_extraction(
        self,
        job_id: str,
        config: dict[str, Any],
        output_dir: Path,
    ) -> ComputeJobResult:
        try:
            target = PipelineExecutionTargetDTO.from_config(config)
            table_name: str = target.object_name
            credential_ref: str = target.credential_ref or self._default_credential_ref
            extraction_query: str | None = target.extraction_query

            if not table_name and not extraction_query:
                raise ValueError(
                    "Either 'object_name' in source_objects or 'extraction_query' must be provided for DuckDB extraction"
                )

            creds = asyncio.run(self._secret_manager.resolve(credential_ref))
            if not isinstance(creds, dict):
                raise ValueError(
                    f"Failed to resolve credentials for credential_ref: {credential_ref!r}"
                )

            parquet_path = output_dir / "data.parquet"

            if creds.get("driver") == "mongodb" or "mongo" in credential_ref:

                async def _extract_mongo() -> int:
                    uri = build_connection_url(creds)
                    client: Any = AsyncIOMotorClient(uri)
                    db_name = creds.get("database")
                    if not db_name and uri:
                        from urllib.parse import urlparse

                        db_name = urlparse(uri).path.lstrip("/").split("?")[0]
                    if not db_name:
                        raise ValueError(
                            "database is required in credentials for MongoDB extraction"
                        )
                    db = client[db_name]
                    coll = db[table_name]
                    cursor = coll.find({})
                    docs = await cursor.to_list(length=10000)
                    now_iso = datetime.now(tz=UTC).isoformat()
                    cleaned_docs = []
                    for d in docs:
                        d_clean = {}
                        for k, v in d.items():
                            d_clean[k] = (
                                str(v)
                                if k == "_id"
                                or not isinstance(
                                    v, (int, float, str, bool, list, dict, type(None))
                                )
                                else v
                            )
                        d_clean["_ingested_at"] = now_iso
                        cleaned_docs.append(d_clean)
                    if not cleaned_docs:
                        raise RuntimeError(
                            f"MongoDB collection '{table_name}' returned zero documents. "
                            "Aborting extraction to prevent empty parquet from masking data absence."
                        )
                    table = pa.Table.from_pylist(cleaned_docs)
                    pq.write_table(table, parquet_path)
                    return len(cleaned_docs)

                asyncio.run(_extract_mongo())
                conn = duckdb.connect(database=":memory:")
            else:
                conn = duckdb.connect(database=":memory:")
                conn.execute("INSTALL postgres; LOAD postgres;")

                db_conn = DatabaseConnectionDTO.from_dict(creds)
                conn.execute(
                    f"ATTACH '{db_conn.to_dsn()}' AS source_db (TYPE POSTGRES, READ_ONLY);"
                )

                schema_name = db_conn.schema_name or (
                    "public" if db_conn.driver == "postgres" else ""
                )

                if extraction_query:
                    query = extraction_query
                elif "." in table_name:
                    query = f"SELECT * FROM source_db.{table_name}"
                elif schema_name:
                    query = f"SELECT * FROM source_db.{schema_name}.{table_name}"
                else:
                    query = f"SELECT * FROM source_db.{table_name}"

                conn.execute(
                    f"COPY (SELECT *, current_timestamp AS _ingested_at FROM ({query})) TO '{parquet_path}' (FORMAT PARQUET);"
                )

            metrics = calculate_parquet_metrics(parquet_path)
            row_count = metrics.get("row_count", 0)

            schema_rows = conn.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')"
            ).fetchall()
            schema = [{"column": col_info[0], "type": col_info[1]} for col_info in schema_rows]

            (output_dir / "metrics.json").write_text(json.dumps(metrics), encoding="utf-8")
            (output_dir / "schema.json").write_text(json.dumps(schema), encoding="utf-8")

            logger.info("DuckDB job concluído: %s (rows=%d)", job_id, row_count)
            return ComputeJobResult(
                job_id=job_id,
                status=JobStatus.SUCCESS,
                output_path=str(parquet_path),
                metrics_path=str(output_dir / "metrics.json"),
                schema_path=str(output_dir / "schema.json"),
            )
        except Exception as exc:
            error_msg = str(exc)
            (output_dir / "error.txt").write_text(error_msg, encoding="utf-8")
            logger.error("DuckDB job falhou: %s - %s", job_id, error_msg)
            return ComputeJobResult(
                job_id=job_id,
                status=JobStatus.FAILED,
                error_message=error_msg,
            )
