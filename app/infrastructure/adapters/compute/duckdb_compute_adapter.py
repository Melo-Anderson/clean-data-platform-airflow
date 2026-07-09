from __future__ import annotations

import asyncio
import json
import logging
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Any

from app.application.shared.secret_manager_port import SecretManagerPort
from app.infrastructure.adapters.compute.job_state import JobState
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
        output_base_dir: str = "/tmp/duckdb_outputs",
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
        """
        Submete a extração DuckDB em background thread e retorna o job_id imediatamente.
        Nunca bloqueia o worker do Airflow.
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
        logger.info("DuckDB job submetido: %s (pipeline=%s)", job_id, pipeline_id)
        return job_id

    def poll_job_status(self, job_id: str) -> ComputeJobResult:
        """
        Verifica estado atual do job. Chamado pela task monitor_compute_job.
        Retorna RUNNING enquanto a thread executa; SUCCESS ou FAILED ao terminar.
        """
        state = self._active_jobs.get(job_id)
        if state is None:
            return ComputeJobResult(
                job_id=job_id,
                status=JobStatus.FAILED,
                error_message=f"job_id desconhecido: {job_id}",
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
        """Cancela um job em execução. Chamado pelo on_failure_callback da DAG."""
        state = self._active_jobs.get(job_id)
        if state is not None:
            state.future.cancel()
            logger.info("DuckDB job cancelado: %s", job_id)

    def _run_extraction(
        self,
        job_id: str,
        config: dict[str, Any],
        output_dir: Path,
    ) -> ComputeJobResult:
        """
        Executa a extração via DuckDB in-memory. Roda em background thread.

        Resolve credenciais via asyncio.run() porque:
        - Esta função é síncrona (chamada por ThreadPoolExecutor)
        - Threads não herdam a event loop do Airflow
        - asyncio.run() cria e fecha uma event loop isolada por chamada
        """
        import duckdb

        credential_ref: str = config["credential_ref"]
        table_name: str = config["source_table"]

        # Resolver credenciais na thread via event loop isolada
        creds = asyncio.run(self._secret_manager.resolve(credential_ref))

        parquet_path = output_dir / "data.parquet"

        conn = duckdb.connect(database=":memory:")
        conn.execute("INSTALL postgres; LOAD postgres;")

        dsn = (
            f"host={creds['host']} port={creds['port']} "
            f"dbname={creds['dbname']} user={creds['username']} password={creds['password']}"
        )
        conn.execute(f"ATTACH '{dsn}' AS source_db (TYPE POSTGRES, READ_ONLY);")
        conn.execute(
            f"COPY (SELECT * FROM source_db.public.{table_name}) "
            f"TO '{parquet_path}' (FORMAT PARQUET);"
        )

        row_count: int = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')"
        ).fetchone()[0]
        schema_rows = conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')"
        ).fetchall()

        metrics = {"row_count": row_count, "bytes_written": parquet_path.stat().st_size}
        schema = [{"column": row[0], "type": row[1]} for row in schema_rows]

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
