# Spec: DuckDB In-Process Local Compute Adapter

*   **Status**: Proposta
*   **Data**: 2026-07-09
*   **Autor**: Antigravity
*   **Pasta de Destino**: `docs/superpowers/specs/`

---

## 1. Contexto e Objetivo

A plataforma suporta orquestração de pipelines onde o processamento de dados é delegado a motores externos (Spark, Dataflow), modelados pelo protocolo `ComputeJobAdapter` e pelo ciclo de vida assíncrono de três tasks:

1. **`submit_compute_job`** — Submete o job e retorna um `job_id` imediatamente.
2. **`monitor_compute_job`** — Fica em polling via sensor até o job terminar.
3. **`validate_compute_execution`** — Valida os arquivos e métricas gerados.

### Objetivo

Implementar o **DuckDB** como motor de compute local (in-process), executando no mesmo container do worker do Airflow. Restrições:

- Deve implementar o protocolo `ComputeJobAdapter` para que os templates de DAG e os use cases **não precisem de nenhuma alteração**.
- A query DuckDB deve executar em **thread de background** para não bloquear o worker do Airflow durante o processamento.
- **Credenciais nunca devem ser embutidas em código ou queries**. Devem ser recuperadas do `OpenBaoClient` em tempo de execução.
- Os outputs (Parquet, `metrics.json`, `schema.json`) devem ser escritos em um diretório local configurável via injeção de dependência.

---

## 2. Arquitetura Proposta

### Localização dos Arquivos

| Arquivo | Tipo | Descrição |
|---|---|---|
| `app/infrastructure/adapters/compute/duckdb_compute_adapter.py` | `[NOVO]` | Implementa `ComputeJobAdapter` com thread pool local |
| `app/infrastructure/adapters/compute/job_state.py` | `[NOVO]` | Value Object `JobState` que rastreia progresso de um job em background |
| `app/infrastructure/compute_job_factory.py` | `[MODIFICAR]` | Registrar `"duckdb"` como engine válido na factory |
| `tests/unit/infrastructure/test_duckdb_compute_adapter.py` | `[NOVO]` | Testes unitários com mocks nomeados |

### Gerenciamento de Threads e Estado

Como o DuckDB roda in-process, não existe API externa para polling. O estado de cada job é rastreado em um atributo de instância `self._active_jobs` — **não em um dicionário global** (que violaria o SRP e tornaria os testes não-isoláveis).

O estado de cada job é representado pelo Value Object `JobState`:

```python
# app/infrastructure/adapters/compute/job_state.py
from __future__ import annotations
from concurrent.futures import Future
from dataclasses import dataclass
from app.infrastructure.airflow_callbacks.compute_job_adapter import ComputeJobResult, JobStatus


@dataclass(frozen=False)   # mutável porque o status é atualizado pelo thread
class JobState:
    """
    Rastreia o estado de um job DuckDB em execução no background.
    Uma instância por job_id em DuckDbComputeAdapter._active_jobs.
    """
    job_id: str
    status: JobStatus
    future: Future[ComputeJobResult]
    result: ComputeJobResult | None = None
    error: str | None = None
```

> `JobState` não é `frozen=True` porque o `status` e `result` são atualizados pela thread de background após a conclusão do job.

### Estrutura de Diretórios de Output

```
{output_base_dir}/
└── {pipeline_id}/
    └── {run_id}/
        ├── data.parquet    ← Output da extração DuckDB
        ├── metrics.json    ← {"row_count": 1500, "bytes_written": 204800}
        └── schema.json     ← [{"column": "id", "type": "INTEGER"}, ...]
```

---

## 3. Spec do Componente `DuckDbComputeAdapter`

### Definição da Classe

```python
# app/infrastructure/adapters/compute/duckdb_compute_adapter.py
from __future__ import annotations

import json
import uuid
from concurrent.futures import ThreadPoolExecutor, Future
from pathlib import Path
from typing import Any

import duckdb

from app.infrastructure.airflow_callbacks.compute_job_adapter import (
    ComputeJobAdapter, ComputeJobResult, JobStatus,
)
from app.infrastructure.adapters.compute.job_state import JobState
from app.infrastructure.vault_client import OpenBaoClient   # porta de credenciais


class DuckDbComputeAdapter:
    """
    Motor de compute local usando DuckDB em thread de background.

    Implementa ComputeJobAdapter para que as tasks da DAG (submit → monitor → validate)
    funcionem sem modificação, independente do engine configurado.

    Credenciais de origem são recuperadas do OpenBaoClient em tempo de execução
    — nunca embutidas em código ou variáveis de ambiente.
    """

    def __init__(
        self,
        vault_client: OpenBaoClient,
        output_base_dir: str = "/tmp/duckdb_outputs",
        max_workers: int = 4,
    ) -> None:
        self._vault_client = vault_client
        self._output_base_dir = Path(output_base_dir)
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._active_jobs: dict[str, JobState] = {}   # estado local, não global

    def submit_job(
        self,
        pipeline_id: str,
        pipeline_type: str,
        config: dict[str, Any],
    ) -> str:
        """
        Inicia a extração DuckDB em background e retorna o job_id imediatamente.
        Nunca bloqueia o worker do Airflow.
        """
        job_id = str(uuid.uuid4())
        output_dir = self._output_base_dir / pipeline_id / job_id
        output_dir.mkdir(parents=True, exist_ok=True)

        future: Future[ComputeJobResult] = self._executor.submit(
            self._run_extraction,
            job_id=job_id,
            pipeline_id=pipeline_id,
            config=config,
            output_dir=output_dir,
        )

        self._active_jobs[job_id] = JobState(
            job_id=job_id,
            status=JobStatus.RUNNING,
            future=future,
        )
        return job_id

    def poll_job_status(self, job_id: str) -> ComputeJobResult:
        """
        Verifica o status atual do job. Chamado pela task `monitor_compute_job`.
        Retorna RUNNING enquanto a thread ainda executa; SUCCESS ou FAILED ao terminar.
        """
        state = self._active_jobs.get(job_id)
        if state is None:
            return ComputeJobResult(job_id=job_id, status=JobStatus.FAILED, error_message=f"job_id desconhecido: {job_id}")

        if not state.future.done():
            return ComputeJobResult(job_id=job_id, status=JobStatus.RUNNING)

        if state.future.exception():
            error = str(state.future.exception())
            return ComputeJobResult(job_id=job_id, status=JobStatus.FAILED, error_message=error)

        return state.future.result()

    def cancel_job(self, job_id: str) -> None:
        """Cancela um job em execução. Chamado pelo on_failure_callback da DAG."""
        state = self._active_jobs.get(job_id)
        if state is not None:
            state.future.cancel()
```

### Lógica de Extração (Executada na Thread de Background)

```python
    def _run_extraction(
        self,
        job_id: str,
        pipeline_id: str,
        config: dict[str, Any],
        output_dir: Path,
    ) -> ComputeJobResult:
        """
        Executa a extração via DuckDB:
        1. Recupera credenciais do OpenBao (nunca do ambiente ou código)
        2. Conecta à fonte via extensão DuckDB adequada
        3. Copia os dados para Parquet
        4. Escreve metrics.json e schema.json para consumo do quality gate
        """
        credential_ref: str = config["credential_ref"]
        table_name: str = config["source_table"]

        # 1. Recuperar credenciais do cofre (OpenBao)
        creds = self._vault_client.get_secret(credential_ref)
        # Retorna: {"host": "...", "port": "...", "dbname": "...", "username": "...", "password": "..."}

        # 2. Conectar ao DuckDB in-memory e carregar extensão
        conn = duckdb.connect(database=":memory:")
        conn.execute("INSTALL postgres; LOAD postgres;")

        # 3. Anexar banco de origem com credenciais resolvidas em tempo de execução
        dsn = (
            f"host={creds['host']} port={creds['port']} "
            f"dbname={creds['dbname']} user={creds['username']} password={creds['password']}"
        )
        conn.execute(f"ATTACH '{dsn}' AS source_db (TYPE POSTGRES, READ_ONLY);")

        # 4. Exportar para Parquet
        parquet_path = output_dir / "data.parquet"
        conn.execute(
            f"COPY (SELECT * FROM source_db.public.{table_name}) "
            f"TO '{parquet_path}' (FORMAT PARQUET);"
        )

        # 5. Calcular e gravar métricas (consumidas pelo quality gate)
        row_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()[0]
        schema_rows = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')").fetchall()

        metrics = {"row_count": row_count, "bytes_written": parquet_path.stat().st_size}
        schema = [{"column": row[0], "type": row[1]} for row in schema_rows]

        (output_dir / "metrics.json").write_text(json.dumps(metrics), encoding="utf-8")
        (output_dir / "schema.json").write_text(json.dumps(schema), encoding="utf-8")

        return ComputeJobResult(
            job_id=job_id,
            status=JobStatus.SUCCESS,
            output_path=str(parquet_path),
            metrics_path=str(output_dir / "metrics.json"),
            schema_path=str(output_dir / "schema.json"),
        )
```

### Atualização da Factory

```python
# app/infrastructure/compute_job_factory.py
def get_compute_adapter(engine: str) -> ComputeJobAdapter:
    if engine == "duckdb":
        from app.infrastructure.adapters.compute.duckdb_compute_adapter import DuckDbComputeAdapter
        from app.infrastructure.vault_client import OpenBaoClient
        return DuckDbComputeAdapter(vault_client=OpenBaoClient())
    return DummyComputeAdapter()
```

---

## 4. Plano de Verificação

### Testes Unitários (F.I.R.S.T)

Os testes usam **classes de mock nomeadas**, não `MagicMock` anônimos, conforme as boas práticas do projeto.

```python
# tests/unit/infrastructure/test_duckdb_compute_adapter.py

class MockVaultClient:
    """Retorna credenciais fake para testes sem I/O real."""
    def get_secret(self, ref: str) -> dict:
        return {"host": "localhost", "port": "5432", "dbname": "test",
                "username": "user", "password": "pass"}

def test_submit_job_returns_immediately():
    """submit_job não deve bloquear — retorna job_id UUID imediatamente."""
    adapter = DuckDbComputeAdapter(vault_client=MockVaultClient(), output_base_dir="/tmp/test")
    job_id = adapter.submit_job("pipeline-1", "ingestion", {"credential_ref": "x", "source_table": "t"})
    assert isinstance(job_id, str)
    assert job_id in adapter._active_jobs

def test_poll_returns_running_while_future_pending():
    """poll_job_status deve retornar RUNNING enquanto a thread ainda executa."""
    ...

def test_poll_returns_success_after_completion():
    """poll_job_status deve retornar SUCCESS com os caminhos de output após conclusão."""
    ...

def test_poll_returns_failed_on_exception():
    """poll_job_status deve retornar FAILED com a mensagem de erro se a thread lançar exceção."""
    ...
```

### Verificação Manual

1. Registrar um pipeline com `engine: "duckdb"` via `POST /pipelines/`.
2. Disparar a execução via `POST /pipelines/{id}/run`.
3. Aguardar a DAG ser parseada e despausada (`_wait_and_unpause_dag`).
4. Verificar no diretório `/tmp/duckdb_outputs/` que os arquivos `data.parquet`, `metrics.json` e `schema.json` foram gerados corretamente.
5. Verificar que o `PipelineRun` foi atualizado para `SUCCESS` ou `quality_failed` após o callback de quality gate.

---

## 5. Decisões de Design e Racionais

| Decisão | Racional |
|---|---|
| `self._active_jobs` (instância, não global) | Variável global viola SRP e impede testes isolados. Instância permite mocks e múltiplos adapters simultâneos. |
| Credenciais via `OpenBaoClient` | Nunca armazenar credenciais em código, queries ou variáveis de ambiente — regra do projeto. |
| `output_base_dir` injetado via `__init__` | Dependency Injection. Facilita testes com diretório temporário (`/tmp/test_...`). |
| `JobState` como dataclass dedicado | Value Object com campos tipados é mais seguro e autodocumentado do que um `dict` anônimo. |
| `ThreadPoolExecutor(max_workers=4)` | Limita paralelismo local para não saturar CPU/memória do worker do Airflow. Configurável via parâmetro. |
| `frozen=False` em `JobState` | O status é atualizado pela thread após conclusão. `frozen=True` quebraria a atualização. |
