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
from app.infrastructure.airflow_callbacks.compute_job_adapter import ComputeJobResult, JobStatus

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
        if job_id not in self._active_jobs:
            return ComputeJobResult(
                job_id=job_id,
                status=JobStatus.FAILED,
                error_message=f"job_id desconhecido: {job_id}",
            )

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
            source_objects = config.get("source_objects")
            first_obj: dict[str, Any] = {}
            if (
                source_objects
                and isinstance(source_objects, list)
                and isinstance(source_objects[0], dict)
            ):
                first_obj = source_objects[0]

            table_name: str = (
                config.get("source_table", "")
                or first_obj.get("object_id", "")
                or first_obj.get("name", "")
            )
            credential_ref: str = (
                config.get("credential_ref", "")
                or first_obj.get("credential_ref", "")
                or self._default_credential_ref
            )
            extraction_query: str | None = config.get("extraction_query") or first_obj.get(
                "extraction_query"
            )

            if not table_name and not extraction_query:
                raise ValueError(
                    "Either 'source_table'/'object_id' or 'extraction_query' must be provided for DuckDB extraction"
                )

            creds = asyncio.run(self._secret_manager.resolve(credential_ref))

            parquet_path = output_dir / "data.parquet"

            if creds.get("driver") == "mongodb" or "mongo" in credential_ref:

                async def _extract_mongo() -> int:
                    uri = (
                        creds.get("uri")
                        or f"mongodb://{creds.get('username')}:{creds.get('password')}@{creds.get('host')}:{creds.get('port', 27017)}/{creds.get('database', 'test_db')}?authSource={creds.get('auth_source', 'admin')}"
                    )
                    client: Any = AsyncIOMotorClient(uri)
                    db_name = creds.get("database", "test_db")
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

                dbname = config.get("database") or creds.get("dbname", creds.get("database"))
                user = config.get("user") or creds.get("username", creds.get("user"))
                password = config.get("password") or creds.get("password")
                host = config.get("host") or creds.get("host")
                port = config.get("port") or creds.get("port")

                dsn = f"host={host} port={port} dbname={dbname} user={user} password={password}"
                conn.execute(f"ATTACH '{dsn}' AS source_db (TYPE POSTGRES, READ_ONLY);")

                schema_name: str = (
                    config.get("source_schema", "")
                    or first_obj.get("schema", "")
                    or creds.get("schema", "")
                    or creds.get("search_path", "")
                    or "public"
                )

                if extraction_query:
                    query = extraction_query
                elif "." in table_name:
                    query = f"SELECT * FROM source_db.{table_name}"
                else:
                    query = f"SELECT * FROM source_db.{schema_name}.{table_name}"

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
