# DuckDB Local Compute Adapter — Plano de Implementação

> **Para agentes de trabalho:** HABILIDADE OBRIGATÓRIA: Use `superpowers:subagent-driven-development` (recomendado) ou `superpowers:executing-plans` para implementar este plano tarefa por tarefa. As etapas usam a sintaxe de caixa de seleção (`- [ ]`) para rastreamento.

**Objetivo:** Implementar o `DuckDbComputeAdapter`, um motor de compute local in-process que executa queries DuckDB em background threads, seguindo o protocolo `ComputeJobAdapter` existente sem alterações nas DAGs ou Use Cases.

**Arquitetura:** O adapter mantém estado local (`self._active_jobs: dict[str, JobState]`) para rastrear futures de background. Credenciais são resolvidas de forma assíncrona via `SecretManagerPort.resolve()` **antes** de submeter a thread (pois threads Python não executam corrotinas), e passadas como parâmetro para a função de extração. O output (Parquet + JSONs de métricas) é gravado em diretório local configurável por injeção de dependência.

**Tech Stack:** `duckdb`, `concurrent.futures.ThreadPoolExecutor`, `pathlib.Path`, `SecretManagerPort` (protocolo existente), `ComputeJobAdapter` (protocolo existente), `pytest-asyncio`.

## Restrições Globais

- Python 3.12+. Tipagem explícita em toda função. Proibido `Any` sem justificativa.
- Proibido acesso direto ao OpenBao ou banco de dados fora do `SecretManagerPort.resolve()`.
- `_active_jobs` deve ser atributo de instância — nunca variável global.
- Testes unitários usam classes de mock nomeadas, não `MagicMock` anônimos.
- Nenhuma credencial em código, query SQL ou variável de ambiente.
- Commits frequentes ao final de cada tarefa.

---

### Tarefa 1: Value Object `JobState`

**Arquivos:**
- Criar: `app/infrastructure/adapters/compute/__init__.py`
- Criar: `app/infrastructure/adapters/compute/job_state.py`
- Testar: `tests/unit/infrastructure/test_duckdb_compute_adapter.py` (setup inicial)

**Interfaces:**
- Consome: `app/infrastructure/airflow_callbacks/compute_job_adapter.py` → `ComputeJobResult`, `JobStatus`
- Produz: `JobState(job_id: str, status: JobStatus, future: Future[ComputeJobResult], result: ComputeJobResult | None, error: str | None)`

- [ ] **Passo 1: Escrever o teste de construção do `JobState`**

```python
# tests/unit/infrastructure/test_duckdb_compute_adapter.py
from concurrent.futures import Future
from app.infrastructure.airflow_callbacks.compute_job_adapter import JobStatus, ComputeJobResult
from app.infrastructure.adapters.compute.job_state import JobState

def test_job_state_initial_status_is_running():
    future: Future[ComputeJobResult] = Future()
    state = JobState(job_id="abc-123", status=JobStatus.RUNNING, future=future)
    assert state.job_id == "abc-123"
    assert state.status == JobStatus.RUNNING
    assert state.result is None
    assert state.error is None
```

- [ ] **Passo 2: Rodar e confirmar falha**

```bash
uv run pytest tests/unit/infrastructure/test_duckdb_compute_adapter.py::test_job_state_initial_status_is_running -v
```
Esperado: `FAILED` com `ModuleNotFoundError`

- [ ] **Passo 3: Criar o `__init__.py` e implementar `JobState`**

```python
# app/infrastructure/adapters/compute/__init__.py
# (arquivo vazio — marca o diretório como pacote Python)
```

```python
# app/infrastructure/adapters/compute/job_state.py
from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass, field

from app.infrastructure.airflow_callbacks.compute_job_adapter import ComputeJobResult, JobStatus


@dataclass
class JobState:
    """
    Rastreia o estado de um job DuckDB em execução no background.
    Não é frozen porque status e result são atualizados ao término da thread.
    Uma instância por job_id em DuckDbComputeAdapter._active_jobs.
    """
    job_id: str
    status: JobStatus
    future: Future[ComputeJobResult]
    result: ComputeJobResult | None = None
    error: str | None = None
```

- [ ] **Passo 4: Rodar e confirmar que passa**

```bash
uv run pytest tests/unit/infrastructure/test_duckdb_compute_adapter.py::test_job_state_initial_status_is_running -v
```
Esperado: `PASSED`

- [ ] **Passo 5: Commit**

```bash
git add app/infrastructure/adapters/compute/ tests/unit/infrastructure/test_duckdb_compute_adapter.py
git commit -m "feat: add JobState value object for DuckDB compute adapter"
```

---

### Tarefa 2: `DuckDbComputeAdapter` — submit e poll

**Arquivos:**
- Criar: `app/infrastructure/adapters/compute/duckdb_compute_adapter.py`
- Testar: `tests/unit/infrastructure/test_duckdb_compute_adapter.py`

**Interfaces:**
- Consome: `JobState` (Tarefa 1), `SecretManagerPort.resolve(ref: str) -> dict[str, str]` (assíncrono), `ComputeJobAdapter` Protocol
- Produz:
  - `DuckDbComputeAdapter.__init__(secret_manager: SecretManagerPort, output_base_dir: str, max_workers: int) -> None`
  - `DuckDbComputeAdapter.submit_job(pipeline_id: str, pipeline_type: str, config: dict[str, Any]) -> str` *(async)*
  - `DuckDbComputeAdapter.poll_job_status(job_id: str) -> ComputeJobResult`
  - `DuckDbComputeAdapter.cancel_job(job_id: str) -> None`

> **Nota de design:** `submit_job` deve ser `async` porque precisa chamar `await secret_manager.resolve()` para buscar as credenciais **antes** de submeter a thread. Threads Python não executam corrotinas. As credenciais resolvidas são passadas como argumento para a função `_run_extraction` que roda na thread.

- [ ] **Passo 1: Escrever testes para `submit_job` e `poll_job_status`**

```python
# Adicionar ao tests/unit/infrastructure/test_duckdb_compute_adapter.py
import asyncio
import pytest
from app.infrastructure.adapters.compute.duckdb_compute_adapter import DuckDbComputeAdapter
from app.infrastructure.airflow_callbacks.compute_job_adapter import JobStatus


class MockSecretManager:
    """Retorna credenciais fake sem I/O real."""
    async def resolve(self, ref: str) -> dict[str, str]:
        return {
            "host": "localhost", "port": "5432", "dbname": "test_db",
            "username": "user", "password": "pass"
        }


@pytest.mark.asyncio
async def test_submit_job_returns_job_id_immediately():
    """submit_job não deve bloquear — retorna UUID string imediatamente."""
    adapter = DuckDbComputeAdapter(
        secret_manager=MockSecretManager(),
        output_base_dir="/tmp/test_duckdb",
    )
    job_id = await adapter.submit_job(
        pipeline_id="pipe-1",
        pipeline_type="ingestion",
        config={"credential_ref": "secret/postgres", "source_table": "orders"},
    )
    assert isinstance(job_id, str)
    assert len(job_id) == 36  # UUID v4


@pytest.mark.asyncio
async def test_submit_job_registers_in_active_jobs():
    """submit_job deve registrar o job em _active_jobs com status RUNNING."""
    adapter = DuckDbComputeAdapter(
        secret_manager=MockSecretManager(),
        output_base_dir="/tmp/test_duckdb",
    )
    job_id = await adapter.submit_job(
        pipeline_id="pipe-1",
        pipeline_type="ingestion",
        config={"credential_ref": "secret/postgres", "source_table": "orders"},
    )
    assert job_id in adapter._active_jobs
    assert adapter._active_jobs[job_id].status == JobStatus.RUNNING


def test_poll_unknown_job_returns_failed():
    """poll_job_status com job_id desconhecido deve retornar FAILED com mensagem clara."""
    adapter = DuckDbComputeAdapter(
        secret_manager=MockSecretManager(),
        output_base_dir="/tmp/test_duckdb",
    )
    result = adapter.poll_job_status("nao-existe")
    assert result.status == JobStatus.FAILED
    assert "nao-existe" in result.error_message
```

- [ ] **Passo 2: Rodar e confirmar falha**

```bash
uv run pytest tests/unit/infrastructure/test_duckdb_compute_adapter.py -v -k "submit or poll_unknown"
```
Esperado: `FAILED` com `ModuleNotFoundError`

- [ ] **Passo 3: Implementar `DuckDbComputeAdapter`**

```python
# app/infrastructure/adapters/compute/duckdb_compute_adapter.py
from __future__ import annotations

import json
import uuid
import logging
from concurrent.futures import ThreadPoolExecutor, Future
from pathlib import Path
from typing import Any

from app.application.shared.secret_manager_port import SecretManagerPort
from app.infrastructure.airflow_callbacks.compute_job_adapter import (
    ComputeJobAdapter, ComputeJobResult, JobStatus,
)
from app.infrastructure.adapters.compute.job_state import JobState

logger = logging.getLogger(__name__)


class DuckDbComputeAdapter:
    """
    Motor de compute local usando DuckDB em thread de background.

    Implementa o mesmo contrato do ComputeJobAdapter (submit → poll → cancel)
    para que as tasks da DAG funcionem sem modificação.

    Credenciais são resolvidas via SecretManagerPort.resolve() antes de
    submeter a thread — threads Python não executam corrotinas.
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

    async def submit_job(
        self,
        pipeline_id: str,
        pipeline_type: str,
        config: dict[str, Any],
    ) -> str:
        """
        Resolve credenciais de forma assíncrona, depois submete a extração
        DuckDB em background thread. Retorna o job_id imediatamente.
        """
        job_id = str(uuid.uuid4())
        output_dir = self._output_base_dir / pipeline_id / job_id
        output_dir.mkdir(parents=True, exist_ok=True)

        # Resolver credenciais no contexto async (antes da thread síncrona)
        credential_ref: str = config["credential_ref"]
        creds = await self._secret_manager.resolve(credential_ref)

        future: Future[ComputeJobResult] = self._executor.submit(
            self._run_extraction,
            job_id=job_id,
            config=config,
            creds=creds,
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
        creds: dict[str, str],
        output_dir: Path,
    ) -> ComputeJobResult:
        """
        Executa a extração via DuckDB in-memory. Roda em background thread.
        Credenciais já foram resolvidas no contexto assíncrono do submit_job.
        """
        import duckdb

        table_name: str = config["source_table"]
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

        row_count = conn.execute(
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
```

- [ ] **Passo 4: Rodar e confirmar que passa**

```bash
uv run pytest tests/unit/infrastructure/test_duckdb_compute_adapter.py -v
```
Esperado: todos os 4 testes `PASSED`

- [ ] **Passo 5: Commit**

```bash
git add app/infrastructure/adapters/compute/duckdb_compute_adapter.py tests/unit/infrastructure/test_duckdb_compute_adapter.py
git commit -m "feat: implement DuckDbComputeAdapter with async submit and sync poll"
```

---

### Tarefa 3: Teste de transição de status após conclusão da thread

**Arquivos:**
- Modificar: `tests/unit/infrastructure/test_duckdb_compute_adapter.py`

**Interfaces:**
- Consome: `DuckDbComputeAdapter` (Tarefa 2)
- Produz: Cobertura dos estados RUNNING→SUCCESS e RUNNING→FAILED

- [ ] **Passo 1: Adicionar testes de transição de estado**

```python
# Adicionar ao tests/unit/infrastructure/test_duckdb_compute_adapter.py
import time
from concurrent.futures import Future


def test_poll_returns_success_after_future_completes():
    """poll_job_status retorna SUCCESS com paths quando a future termina com êxito."""
    adapter = DuckDbComputeAdapter(
        secret_manager=MockSecretManager(),
        output_base_dir="/tmp/test_duckdb",
    )

    # Simular um future já completado com sucesso
    future: Future[ComputeJobResult] = Future()
    expected = ComputeJobResult(
        job_id="test-job",
        status=JobStatus.SUCCESS,
        output_path="/tmp/data.parquet",
        metrics_path="/tmp/metrics.json",
        schema_path="/tmp/schema.json",
    )
    future.set_result(expected)

    from app.infrastructure.adapters.compute.job_state import JobState
    adapter._active_jobs["test-job"] = JobState(
        job_id="test-job", status=JobStatus.RUNNING, future=future
    )

    result = adapter.poll_job_status("test-job")
    assert result.status == JobStatus.SUCCESS
    assert result.output_path == "/tmp/data.parquet"


def test_poll_returns_failed_when_future_raises():
    """poll_job_status retorna FAILED com a mensagem de erro quando a thread lança exceção."""
    adapter = DuckDbComputeAdapter(
        secret_manager=MockSecretManager(),
        output_base_dir="/tmp/test_duckdb",
    )

    future: Future[ComputeJobResult] = Future()
    future.set_exception(RuntimeError("Conexão com banco recusada"))

    from app.infrastructure.adapters.compute.job_state import JobState
    adapter._active_jobs["fail-job"] = JobState(
        job_id="fail-job", status=JobStatus.RUNNING, future=future
    )

    result = adapter.poll_job_status("fail-job")
    assert result.status == JobStatus.FAILED
    assert "Conexão com banco recusada" in result.error_message
```

- [ ] **Passo 2: Rodar e confirmar que passa**

```bash
uv run pytest tests/unit/infrastructure/test_duckdb_compute_adapter.py -v
```
Esperado: todos os 6 testes `PASSED`

- [ ] **Passo 3: Commit**

```bash
git add tests/unit/infrastructure/test_duckdb_compute_adapter.py
git commit -m "test: add status transition tests for DuckDbComputeAdapter poll"
```

---

### Tarefa 4: Registrar `"duckdb"` na `compute_job_factory`

**Arquivos:**
- Modificar: `app/infrastructure/compute_job_factory.py`
- Testar: `tests/unit/infrastructure/test_duckdb_compute_adapter.py`

**Interfaces:**
- Consome: `DuckDbComputeAdapter` (Tarefa 2), `SecretManagerPort`, `secret_manager_factory`
- Produz: `get_compute_adapter("duckdb") -> DuckDbComputeAdapter`

- [ ] **Passo 1: Escrever teste para a factory**

```python
# Adicionar ao tests/unit/infrastructure/test_duckdb_compute_adapter.py
from app.infrastructure.compute_job_factory import get_compute_adapter
from app.infrastructure.adapters.compute.duckdb_compute_adapter import DuckDbComputeAdapter


def test_factory_returns_duckdb_adapter_for_duckdb_engine():
    """get_compute_adapter('duckdb') deve retornar uma instância de DuckDbComputeAdapter."""
    adapter = get_compute_adapter("duckdb")
    assert isinstance(adapter, DuckDbComputeAdapter)
```

- [ ] **Passo 2: Rodar e confirmar falha**

```bash
uv run pytest tests/unit/infrastructure/test_duckdb_compute_adapter.py::test_factory_returns_duckdb_adapter_for_duckdb_engine -v
```
Esperado: `FAILED` — factory retorna `DummyComputeAdapter`

- [ ] **Passo 3: Atualizar a factory**

```python
# app/infrastructure/compute_job_factory.py
from __future__ import annotations

from typing import Any

from app.infrastructure.airflow_callbacks.compute_job_adapter import ComputeJobAdapter


class DummyComputeAdapter:
    def submit_job(self, pipeline_id: str, pipeline_type: str, config: dict[str, Any]) -> str:
        return "dummy-job-123"

    def poll_job_status(self, job_id: str) -> Any:
        pass

    def cancel_job(self, job_id: str) -> None:
        pass


def get_compute_adapter(engine: str) -> ComputeJobAdapter:
    if engine == "duckdb":
        from app.infrastructure.adapters.compute.duckdb_compute_adapter import DuckDbComputeAdapter
        from app.infrastructure.adapters.secrets.secret_manager_factory import get_secret_manager
        return DuckDbComputeAdapter(secret_manager=get_secret_manager())
    return DummyComputeAdapter()


def get_transform_adapter(engine: str) -> ComputeJobAdapter:
    return DummyComputeAdapter()
```

- [ ] **Passo 4: Rodar todos os testes da suite unitária**

```bash
uv run pytest tests/unit/ -v
```
Esperado: todos os testes unitários `PASSED` (nenhuma regressão)

- [ ] **Passo 5: Commit**

```bash
git add app/infrastructure/compute_job_factory.py tests/unit/infrastructure/test_duckdb_compute_adapter.py
git commit -m "feat: register duckdb engine in compute_job_factory"
```

---

### Tarefa 5: Verificação de tipos e lint

**Arquivos:**
- Verificar: todos os arquivos criados/modificados nas Tarefas 1–4

- [ ] **Passo 1: Rodar mypy**

```bash
uv run mypy app/infrastructure/adapters/compute/ app/infrastructure/compute_job_factory.py
```
Esperado: `Success: no issues found`

- [ ] **Passo 2: Rodar ruff**

```bash
uv run ruff check app/infrastructure/adapters/compute/ app/infrastructure/compute_job_factory.py
```
Esperado: nenhum erro de linting

- [ ] **Passo 3: Corrigir qualquer erro reportado e recomitar**

```bash
uv run ruff check app/infrastructure/adapters/compute/ --fix
git add -A
git commit -m "fix: address mypy/ruff issues in DuckDB adapter"
```

---

## Auto-revisão do Plano contra a Spec

| Requisito da Spec | Coberto por |
|---|---|
| Implementar `ComputeJobAdapter` sem alterar DAGs/UseCases | Tarefa 2 — classe segue o mesmo contrato de submit/poll/cancel |
| Thread de background para não bloquear o Airflow worker | Tarefa 2 — `ThreadPoolExecutor.submit()` retorna imediatamente |
| Credenciais via `SecretManagerPort`, nunca hardcoded | Tarefa 2 — `await secret_manager.resolve()` antes da thread |
| `self._active_jobs` de instância, não global | Tarefa 2 — `self._active_jobs: dict[str, JobState] = {}` |
| `JobState` como dataclass tipado | Tarefa 1 |
| `output_base_dir` injetado via `__init__` | Tarefa 2 |
| Gravar `data.parquet`, `metrics.json`, `schema.json` | Tarefa 2 — `_run_extraction` |
| Registrar engine `"duckdb"` na factory | Tarefa 4 |
| Testes com classes mock nomeadas (F.I.R.S.T) | Tarefas 2 e 3 — `MockSecretManager` nomeado |
| Testes de transição RUNNING→SUCCESS e RUNNING→FAILED | Tarefa 3 |
| Verificação de tipos e lint | Tarefa 5 |
