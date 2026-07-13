# ETL Pipeline Rigor — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implementar lógica real nos callbacks ETL: classificação de drift de schema com `SchemaDiffer` e um adapter genérico de transformação simulada (`DbtComputeAdapter`) conectado ao factory existente, garantindo que o pipeline de ETL bloqueie execuções com mudanças de schema incompatíveis.

**Architecture:** O `DriftClassifier` deixa de ser um stub e passa a delegar ao `SchemaDiffer` (domain service já existente), sem violar Clean Architecture — toda lógica de negócio fica em `domain/`, o classifier em `infrastructure/` apenas faz a conversão de dicionário → objetos de domínio. O `DbtComputeAdapter` segue o mesmo padrão do `DuckDbComputeAdapter`: implementa `ComputeJobAdapter` (Protocol), fica em `app/infrastructure/adapters/compute/` e expõe métricas genéricas (não dbt-específicas) via `ComputeJobResult`. A integração dos callbacks ETL reusa o adapter via `get_transform_adapter()`.

**Tech Stack:** Python 3.12+, `uv`, `pytest`, `dataclasses`, módulos existentes: `app.domain.discovery.services.schema_differ.SchemaDiffer`, `app.domain.discovery.schema_snapshot.SchemaSnapshot`, `app.domain.discovery.schema_field.SchemaField`, `app.domain.discovery.drift_change_type.DriftChangeType`, `app.infrastructure.airflow_callbacks.compute_job_adapter.ComputeJobAdapter | ComputeJobResult | JobStatus`, `app.domain.shared.exceptions.PlatformValidationError`.

## Global Constraints

- Funções: 4–20 linhas máximo
- Arquivos: máximo 300 linhas
- `domain/` não pode importar nada de `infrastructure/`
- `infrastructure/` pode importar `domain/` mas não pode conter lógica de negócio
- Sem `MagicMock` anônimo em testes — usar objetos explícitos
- Tipagem explícita em todas as assinaturas; proibido `Any` sem justificativa
- TDD: testes antes de implementação; commits frequentes
- Mensagens de erro incluem o valor ofensivo: `raise PlatformValidationError(f"Incompatible drift: {details}")`
- `get_transform_adapter(engine)` em `compute_job_factory.py` deve mapear `"dbt"` → `DbtComputeAdapter`; qualquer engine desconhecida retorna `DummyComputeAdapter`

---

## Task 1: DriftClassifier Real com SchemaDiffer

**Files:**
- Modify: `app/infrastructure/drift_classifier.py`
- Test: `tests/unit/infrastructure/test_drift_classifier.py`

**Interfaces:**
- Consumes: `SchemaDiffer.diff(prev: SchemaSnapshot, curr: SchemaSnapshot) -> list[DriftEvent]`
- Consumes: `SchemaField(name: str, source_type: str, normalized_type: str, nullable: bool)`
- Consumes: `SchemaSnapshot(object_id: str, fields: list[SchemaField])`
- Consumes: `DriftChangeType.FIELD_REMOVED`, `DriftChangeType.TYPE_INCOMPATIBLE`
- Produces: `DriftClassifier().classify_models(source_models: dict) -> dict[str, Any]`
  - `source_models` formato:
    ```python
    {
        "prev": {"object_id": str, "fields": [{"name": str, "type": str, "nullable": bool}]},
        "curr": {"object_id": str, "fields": [{"name": str, "type": str, "nullable": bool}]}
    }
    ```
  - Retorna `{"can_proceed": bool, "blocked_reason": str}`

> [!NOTE]
> O `DriftClassifier` é um **adapter de infraestrutura**: sua única responsabilidade é converter o dicionário raw do callback Airflow em objetos de domínio e delegar ao `SchemaDiffer`. Nenhuma lógica de negócio (regras de compatibilidade) entra aqui — isso já está em `SchemaDiffer` + `SchemaField.is_compatible_with`.

- [ ] **Step 1: Escrever testes unitários**

```python
# tests/unit/infrastructure/test_drift_classifier.py
from __future__ import annotations
from app.infrastructure.drift_classifier import DriftClassifier


def _snap(fields: list[dict]) -> dict:
    return {"object_id": "orders", "fields": fields}


def test_classify_models_returns_can_proceed_when_no_drift() -> None:
    """Schemas idênticos não produzem bloqueio."""
    fields = [{"name": "id", "type": "integer", "nullable": False}]
    result = DriftClassifier().classify_models({"prev": _snap(fields), "curr": _snap(fields)})
    assert result["can_proceed"] is True
    assert result["blocked_reason"] == ""


def test_classify_models_allows_field_addition() -> None:
    """Campo adicionado é drift compatível — não bloqueia."""
    prev = _snap([{"name": "id", "type": "integer", "nullable": False}])
    curr = _snap([
        {"name": "id", "type": "integer", "nullable": False},
        {"name": "name", "type": "string", "nullable": True},
    ])
    result = DriftClassifier().classify_models({"prev": prev, "curr": curr})
    assert result["can_proceed"] is True


def test_classify_models_allows_type_widening() -> None:
    """integer → bigint é widening compatível — não bloqueia."""
    prev = _snap([{"name": "amount", "type": "integer", "nullable": True}])
    curr = _snap([{"name": "amount", "type": "bigint", "nullable": True}])
    result = DriftClassifier().classify_models({"prev": prev, "curr": curr})
    assert result["can_proceed"] is True


def test_classify_models_blocks_on_field_removal() -> None:
    """Campo removido é drift incompatível — bloqueia com motivo descritivo."""
    prev = _snap([
        {"name": "id", "type": "integer", "nullable": False},
        {"name": "amount", "type": "float", "nullable": True},
    ])
    curr = _snap([{"name": "id", "type": "integer", "nullable": False}])
    result = DriftClassifier().classify_models({"prev": prev, "curr": curr})
    assert result["can_proceed"] is False
    assert "amount" in result["blocked_reason"]


def test_classify_models_blocks_on_incompatible_type_change() -> None:
    """integer → string é tipo incompatível — bloqueia."""
    prev = _snap([{"name": "order_id", "type": "integer", "nullable": False}])
    curr = _snap([{"name": "order_id", "type": "string", "nullable": False}])
    result = DriftClassifier().classify_models({"prev": prev, "curr": curr})
    assert result["can_proceed"] is False
    assert "order_id" in result["blocked_reason"]


def test_classify_models_empty_prev_or_curr_returns_can_proceed() -> None:
    """Sem snapshots anteriores ou atuais, não há o que comparar — não bloqueia."""
    result = DriftClassifier().classify_models({})
    assert result["can_proceed"] is True


def test_classify_models_preserves_all_blocked_fields_in_reason() -> None:
    """Quando múltiplos campos estão com drift incompatível, todos aparecem no blocked_reason."""
    prev = _snap([
        {"name": "a", "type": "integer", "nullable": True},
        {"name": "b", "type": "float", "nullable": True},
    ])
    curr = _snap([
        {"name": "a", "type": "string", "nullable": True},  # incompatible
        # "b" removed
    ])
    result = DriftClassifier().classify_models({"prev": prev, "curr": curr})
    assert result["can_proceed"] is False
    assert "a" in result["blocked_reason"]
    assert "b" in result["blocked_reason"]
```

- [ ] **Step 2: Rodar testes para confirmar FAIL**

```bash
uv run pytest tests/unit/infrastructure/test_drift_classifier.py -v
```
Expected: múltiplos `FAILED` — `DriftClassifier.classify_models` ainda retorna stub.

- [ ] **Step 3: Implementar `DriftClassifier` real**

```python
# app/infrastructure/drift_classifier.py
from __future__ import annotations

from typing import Any

from app.domain.discovery.drift_change_type import DriftChangeType
from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.discovery.services.schema_differ import SchemaDiffer

_BLOCKING_CHANGE_TYPES = frozenset({
    DriftChangeType.FIELD_REMOVED,
    DriftChangeType.TYPE_INCOMPATIBLE,
})


def _parse_snapshot(data: dict[str, Any]) -> SchemaSnapshot:
    """Convert raw dict payload to SchemaSnapshot domain object."""
    fields = [
        SchemaField(
            name=f["name"],
            source_type=f["type"],
            normalized_type=f["type"],
            nullable=f.get("nullable", True),
        )
        for f in data.get("fields", [])
    ]
    return SchemaSnapshot(object_id=data.get("object_id", "unknown"), fields=fields)


class DriftClassifier:
    """Classifies schema drift between two snapshots using the domain SchemaDiffer.

    Infrastructure adapter: converts raw dict payloads into domain objects and
    delegates comparison logic entirely to SchemaDiffer. No business logic here.

    Example:
        result = DriftClassifier().classify_models({
            "prev": {"object_id": "orders", "fields": [{"name": "id", "type": "integer"}]},
            "curr": {"object_id": "orders", "fields": [{"name": "id", "type": "string"}]},
        })
        # result == {"can_proceed": False, "blocked_reason": "id: type_incompatible"}
    """

    def __init__(self) -> None:
        self._differ = SchemaDiffer()

    def classify_models(self, source_models: dict[str, Any]) -> dict[str, Any]:
        """Classify schema changes, returning whether the ETL can proceed.

        Args:
            source_models: Dict with optional "prev" and "curr" snapshot dicts.

        Returns:
            {"can_proceed": bool, "blocked_reason": str}
        """
        prev_data = source_models.get("prev")
        curr_data = source_models.get("curr")

        if not prev_data or not curr_data:
            return {"can_proceed": True, "blocked_reason": ""}

        events = self._differ.diff(_parse_snapshot(prev_data), _parse_snapshot(curr_data))

        blocking = [e for e in events if e.change_type in _BLOCKING_CHANGE_TYPES]

        if not blocking:
            return {"can_proceed": True, "blocked_reason": ""}

        reason = "; ".join(
            f"{e.field_name}: {e.change_type.value}" for e in blocking
        )
        return {"can_proceed": False, "blocked_reason": f"Incompatible drift detected: {reason}"}

    def classify(self, schema_snapshot: dict[str, Any], policy: str) -> dict[str, Any]:
        """Legacy stub for backward compatibility. Does not block."""
        return {"can_proceed": True, "blocked_reason": ""}
```

- [ ] **Step 4: Rodar testes para confirmar PASS**

```bash
uv run pytest tests/unit/infrastructure/test_drift_classifier.py -v
```
Expected: todos `PASSED`.

- [ ] **Step 5: Commit**

```bash
git add app/infrastructure/drift_classifier.py tests/unit/infrastructure/test_drift_classifier.py
git commit -m "feat(etl): implement real DriftClassifier using SchemaDiffer domain service"
```

---

## Task 2: DbtComputeAdapter — Adapter Genérico de Transformação

**Files:**
- Create: `app/infrastructure/adapters/compute/dbt_compute_adapter.py`
- Modify: `app/infrastructure/compute_job_factory.py`
- Test: `tests/unit/infrastructure/adapters/test_dbt_compute_adapter.py`

**Interfaces:**
- Consumes: `ComputeJobAdapter` Protocol de `app.infrastructure.airflow_callbacks.compute_job_adapter`
- Consumes: `ComputeJobResult(job_id, status, metrics_path, output_path, error_message)`
- Consumes: `JobStatus.RUNNING`, `JobStatus.SUCCESS`, `JobStatus.FAILED`
- Produces: `DbtComputeAdapter` — implementa `ComputeJobAdapter`
  - `submit_job(pipeline_id, pipeline_type, config) -> str` — retorna `"dbt-job-<uuid>"`
  - `poll_job_status(job_id) -> ComputeJobResult` — RUNNING na 1ª chamada, SUCCESS na 2ª+
  - `cancel_job(job_id) -> None`
- Produces: `get_transform_adapter("dbt") -> DbtComputeAdapter` (factory atualizada)

> [!NOTE]
> O `DbtComputeAdapter` **não expõe métricas específicas de dbt** (models_run, tests_passed). O `ComputeJobResult` existente já define os campos genéricos: `metrics_path`, `output_path`. O adapter popula esses campos com caminhos simulados. Métricas detalhadas de dbt são responsabilidade do arquivo de métricas (lido em `read_transformation_metrics`), não do adapter em si — isso respeita SRP.

- [ ] **Step 1: Escrever testes unitários**

```python
# tests/unit/infrastructure/adapters/test_dbt_compute_adapter.py
from __future__ import annotations

from app.infrastructure.adapters.compute.dbt_compute_adapter import DbtComputeAdapter
from app.infrastructure.airflow_callbacks.compute_job_adapter import JobStatus


def test_submit_job_returns_dbt_prefixed_job_id() -> None:
    """submit_job deve retornar um job_id com prefixo 'dbt-job-'."""
    adapter = DbtComputeAdapter()
    job_id = adapter.submit_job(
        pipeline_id="p-001",
        pipeline_type="etl",
        config={"ref": "models/orders.sql", "engine": "dbt"},
    )
    assert job_id.startswith("dbt-job-")


def test_poll_job_status_is_running_on_first_poll() -> None:
    """Primeiro poll retorna RUNNING (simula job assíncrono em execução)."""
    adapter = DbtComputeAdapter()
    job_id = adapter.submit_job("p-001", "etl", {"ref": "models/x.sql"})
    result = adapter.poll_job_status(job_id)
    assert result.status == JobStatus.RUNNING


def test_poll_job_status_is_success_on_second_poll() -> None:
    """Segundo poll retorna SUCCESS com metrics_path preenchido."""
    adapter = DbtComputeAdapter()
    job_id = adapter.submit_job("p-001", "etl", {"ref": "models/x.sql"})
    adapter.poll_job_status(job_id)  # 1st: RUNNING
    result = adapter.poll_job_status(job_id)  # 2nd: SUCCESS
    assert result.status == JobStatus.SUCCESS
    assert result.metrics_path is not None
    assert job_id in result.metrics_path


def test_cancel_job_removes_job_state() -> None:
    """cancel_job elimina o estado do job; poll subsequente retorna FAILED."""
    adapter = DbtComputeAdapter()
    job_id = adapter.submit_job("p-001", "etl", {"ref": "models/x.sql"})
    adapter.cancel_job(job_id)
    result = adapter.poll_job_status(job_id)
    assert result.status == JobStatus.FAILED


def test_poll_unknown_job_returns_failed() -> None:
    """Poll de job inexistente retorna FAILED sem exceção."""
    adapter = DbtComputeAdapter()
    result = adapter.poll_job_status("non-existent-job-id")
    assert result.status == JobStatus.FAILED


def test_get_transform_adapter_returns_dbt_adapter_for_dbt_engine() -> None:
    """Factory deve retornar DbtComputeAdapter para engine 'dbt'."""
    from app.infrastructure.compute_job_factory import get_transform_adapter
    adapter = get_transform_adapter("dbt")
    assert isinstance(adapter, DbtComputeAdapter)
```

- [ ] **Step 2: Rodar testes para confirmar FAIL**

```bash
uv run pytest tests/unit/infrastructure/adapters/test_dbt_compute_adapter.py -v
```
Expected: `ImportError` ou `FAILED` — `dbt_compute_adapter` não existe.

- [ ] **Step 3: Criar `app/infrastructure/adapters/compute/dbt_compute_adapter.py`**

```python
# app/infrastructure/adapters/compute/dbt_compute_adapter.py
from __future__ import annotations

import logging
import uuid

from typing import Any

from app.infrastructure.airflow_callbacks.compute_job_adapter import ComputeJobResult, JobStatus

logger = logging.getLogger(__name__)


class DbtComputeAdapter:
    """Simulates async dbt/Dataform transformation job lifecycle.

    Implements ComputeJobAdapter protocol. Each submitted job requires at least
    two poll cycles (simulating async CLI execution time) before completing.
    metrics_path is populated on SUCCESS with a local path for downstream tasks.

    Example:
        adapter = DbtComputeAdapter()
        job_id = adapter.submit_job("p-001", "etl", {"ref": "models/orders.sql"})
        result = adapter.poll_job_status(job_id)  # RUNNING
        result = adapter.poll_job_status(job_id)  # SUCCESS
    """

    def __init__(self) -> None:
        self._jobs: dict[str, int] = {}

    def submit_job(self, pipeline_id: str, pipeline_type: str, config: dict[str, Any]) -> str:
        """Register a new transform job and return its unique job_id."""
        job_id = f"dbt-job-{uuid.uuid4()}"
        self._jobs[job_id] = 0
        logger.info("dbt transform job submitted: %s (pipeline=%s)", job_id, pipeline_id)
        return job_id

    def poll_job_status(self, job_id: str) -> ComputeJobResult:
        """Poll job state. Returns RUNNING on first call, SUCCESS on second+, FAILED if unknown."""
        if job_id not in self._jobs:
            return ComputeJobResult(job_id=job_id, status=JobStatus.FAILED,
                                    error_message=f"Unknown job: {job_id}")

        self._jobs[job_id] += 1
        attempts = self._jobs[job_id]

        if attempts < 2:
            return ComputeJobResult(job_id=job_id, status=JobStatus.RUNNING)

        metrics_path = f"/tmp/dbt_outputs/{job_id}/metrics.json"
        return ComputeJobResult(job_id=job_id, status=JobStatus.SUCCESS, metrics_path=metrics_path)

    def cancel_job(self, job_id: str) -> None:
        """Cancel and remove job state."""
        self._jobs.pop(job_id, None)
        logger.info("dbt transform job cancelled: %s", job_id)
```

- [ ] **Step 4: Atualizar `app/infrastructure/compute_job_factory.py`**

```python
# app/infrastructure/compute_job_factory.py
from __future__ import annotations

from typing import Any

from app.infrastructure.airflow_callbacks.compute_job_adapter import (
    ComputeJobAdapter,
    ComputeJobResult,
    JobStatus,
)


class DummyComputeAdapter:
    def submit_job(self, pipeline_id: str, pipeline_type: str, config: dict[str, Any]) -> str:
        return "dummy-job-123"

    def poll_job_status(self, job_id: str) -> ComputeJobResult:
        return ComputeJobResult(job_id=job_id, status=JobStatus.SUCCESS)

    def cancel_job(self, job_id: str) -> None:
        pass


def get_compute_adapter(engine: str) -> ComputeJobAdapter:
    if engine == "duckdb":
        from app.config import get_settings
        from app.infrastructure.adapters.compute.duckdb_compute_adapter import DuckDbComputeAdapter
        from app.infrastructure.adapters.secrets.secret_manager_factory import get_secret_manager

        return DuckDbComputeAdapter(secret_manager=get_secret_manager(get_settings()))
    return DummyComputeAdapter()


def get_transform_adapter(engine: str) -> ComputeJobAdapter:
    """Return the appropriate transform adapter for the given engine.

    Args:
        engine: Transform engine name. Supported: "dbt". Falls back to DummyComputeAdapter.
    """
    if engine == "dbt":
        from app.infrastructure.adapters.compute.dbt_compute_adapter import DbtComputeAdapter
        return DbtComputeAdapter()
    return DummyComputeAdapter()
```

- [ ] **Step 5: Rodar testes para confirmar PASS**

```bash
uv run pytest tests/unit/infrastructure/adapters/test_dbt_compute_adapter.py -v
```
Expected: todos `PASSED`.

- [ ] **Step 6: Commit**

```bash
git add app/infrastructure/adapters/compute/dbt_compute_adapter.py \
        app/infrastructure/compute_job_factory.py \
        tests/unit/infrastructure/adapters/test_dbt_compute_adapter.py
git commit -m "feat(etl): add DbtComputeAdapter and wire into get_transform_adapter factory"
```

---

## Task 3: Integração nos ETL Callbacks (Gate de Drift Real)

**Files:**
- Modify: `app/infrastructure/airflow_callbacks/etl_callbacks.py`
- Test: `tests/unit/infrastructure/test_etl_callbacks.py`

**Interfaces:**
- Consumes: `DriftClassifier().classify_models(source_models: dict) -> {"can_proceed": bool, "blocked_reason": str}` (Task 1)
- Consumes: `get_transform_adapter(engine: str) -> ComputeJobAdapter` (Task 2)
- Consumes: `PlatformValidationError` de `app.domain.shared.exceptions`
- Produces: `classify_schema_changes(*, source_models: dict) -> dict[str, Any]` — levanta `PlatformValidationError` em drift incompatível
- Produces: `submit_transformation_job(*, pipeline_id, transform_engine, transform_ref, compute_config) -> {"job_id": str, "submitted_at": str}` — usa adapter real

- [ ] **Step 1: Escrever testes unitários**

```python
# tests/unit/infrastructure/test_etl_callbacks.py
from __future__ import annotations

import pytest


def test_classify_schema_changes_passes_on_compatible_schemas() -> None:
    """Schemas compatíveis não levantam exceção e retornam can_proceed=True."""
    from app.infrastructure.airflow_callbacks.etl_callbacks import classify_schema_changes

    source_models = {
        "prev": {"object_id": "orders", "fields": [{"name": "id", "type": "integer"}]},
        "curr": {
            "object_id": "orders",
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "name", "type": "string"},
            ],
        },
    }
    result = classify_schema_changes(source_models=source_models)
    assert result["can_proceed"] is True


def test_classify_schema_changes_raises_on_incompatible_drift() -> None:
    """Drift incompatível levanta PlatformValidationError com campo identificado."""
    from app.domain.shared.exceptions import PlatformValidationError
    from app.infrastructure.airflow_callbacks.etl_callbacks import classify_schema_changes

    source_models = {
        "prev": {"object_id": "orders", "fields": [{"name": "amount", "type": "integer"}]},
        "curr": {"object_id": "orders", "fields": [{"name": "amount", "type": "string"}]},
    }
    with pytest.raises(PlatformValidationError, match="amount"):
        classify_schema_changes(source_models=source_models)


def test_submit_transformation_job_returns_job_id_and_timestamp() -> None:
    """submit_transformation_job deve retornar job_id e submitted_at ISO."""
    from app.infrastructure.airflow_callbacks.etl_callbacks import submit_transformation_job

    result = submit_transformation_job(
        pipeline_id="p-001",
        transform_engine="dbt",
        transform_ref="models/orders.sql",
        compute_config={"engine": "dbt", "num_workers": 2},
    )
    assert "job_id" in result
    assert result["job_id"].startswith("dbt-job-")
    assert "submitted_at" in result


def test_submit_transformation_job_falls_back_for_unknown_engine() -> None:
    """Engine desconhecida retorna job_id do DummyAdapter sem exceção."""
    from app.infrastructure.airflow_callbacks.etl_callbacks import submit_transformation_job

    result = submit_transformation_job(
        pipeline_id="p-001",
        transform_engine="dataform",
        transform_ref="workflows/orders",
        compute_config={},
    )
    assert "job_id" in result
    assert "submitted_at" in result
```

- [ ] **Step 2: Rodar testes para confirmar FAIL**

```bash
uv run pytest tests/unit/infrastructure/test_etl_callbacks.py -v
```
Expected: `FAILED` — `classify_schema_changes` ainda retorna stub; `submit_transformation_job` não usa adapter correto.

- [ ] **Step 3: Atualizar `app/infrastructure/airflow_callbacks/etl_callbacks.py`**

```python
# app/infrastructure/airflow_callbacks/etl_callbacks.py
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.domain.shared.exceptions import PlatformValidationError
from app.infrastructure.compute_job_factory import get_transform_adapter
from app.infrastructure.drift_classifier import DriftClassifier


def validate_source_models(*, pipeline_id: str, source_asset_id: str) -> dict[str, Any]:
    """Validate that all source dbt/Dataform models exist and are fresh."""
    return {"valid": True}


def classify_schema_changes(*, source_models: dict[str, Any]) -> dict[str, Any]:
    """Classify schema drift between previous and current model snapshots.

    Raises:
        PlatformValidationError: If incompatible drift is detected (field removed,
            type incompatible). The error message identifies the offending fields.
    """
    result = DriftClassifier().classify_models(source_models=source_models)
    if not result["can_proceed"]:
        raise PlatformValidationError(result["blocked_reason"])
    return result


def submit_transformation_job(
    *,
    pipeline_id: str,
    transform_engine: str,
    transform_ref: str,
    compute_config: dict[str, Any],
) -> dict[str, str]:
    """Submit a transformation job via the engine-specific compute adapter.

    Args:
        pipeline_id: Platform pipeline identifier.
        transform_engine: Engine name, e.g. "dbt". Determines adapter selection.
        transform_ref: Model reference path (e.g. "models/orders.sql").
        compute_config: Engine-specific configuration dict.

    Returns:
        {"job_id": str, "submitted_at": ISO timestamp str}
    """
    adapter = get_transform_adapter(transform_engine)
    job_id = adapter.submit_job(
        pipeline_id=pipeline_id,
        pipeline_type="etl",
        config={"ref": transform_ref, **compute_config},
    )
    return {"job_id": job_id, "submitted_at": datetime.now(tz=UTC).isoformat()}


def publish_documentation(*, pipeline_id: str, transform_ref: str) -> None:
    """Publish updated dbt/Dataform docs to the catalog adapter."""
```

- [ ] **Step 4: Rodar testes para confirmar PASS**

```bash
uv run pytest tests/unit/infrastructure/test_etl_callbacks.py -v
```
Expected: todos `PASSED`.

- [ ] **Step 5: Commit**

```bash
git add app/infrastructure/airflow_callbacks/etl_callbacks.py \
        tests/unit/infrastructure/test_etl_callbacks.py
git commit -m "feat(etl): wire real DriftClassifier and DbtComputeAdapter into ETL callbacks"
```

---

## Task 4: Suite Completa e Verificação Final

**Files:**
- (sem alterações — apenas execução de suite completa)

- [ ] **Step 1: Rodar toda a suite (exceto e2e) com cobertura**

```bash
uv run pytest -m "not e2e" --cov=app --cov-report=term-missing --tb=short -v
```
Expected: ≥ 85% cobertura, sem regressões.

- [ ] **Step 2: Rodar mypy e linters**

```bash
uv run mypy app/
uv run ruff check app/ tests/
uv run ruff format --check app/ tests/
```
Expected: sem erros de tipo nem de lint.

- [ ] **Step 3: Commit final de verificação**

```bash
git add .
git commit -m "test: full suite verification after ETL pipeline rigor implementation"
```

---

## Self-Review

**Spec coverage:**
| Requisito | Task | Status |
|---|---|---|
| DriftClassifier real usando SchemaDiffer | Task 1 | ✅ |
| classify_models recebe snapshots via dict params | Task 1 | ✅ |
| Bloqueia FIELD_REMOVED e TYPE_INCOMPATIBLE | Task 1 | ✅ |
| Permite FIELD_ADDED e TYPE_WIDENED | Task 1 (testes) | ✅ |
| DbtComputeAdapter implementa ComputeJobAdapter | Task 2 | ✅ |
| poll_job_status: RUNNING → SUCCESS após 2 polls | Task 2 | ✅ |
| Métricas genéricas (metrics_path), não dbt-específicas | Task 2 | ✅ |
| get_transform_adapter("dbt") → DbtComputeAdapter | Task 2 | ✅ |
| classify_schema_changes levanta PlatformValidationError | Task 3 | ✅ |
| submit_transformation_job usa adapter correto | Task 3 | ✅ |
| Engine desconhecida cai no DummyAdapter | Task 3 (teste) | ✅ |
| Suite completa passando ≥ 85% cobertura | Task 4 | ✅ |

**Descartados / Adaptados:**
- Métricas dbt-específicas (models_run, tests_passed) removidas do adapter — violavam SRP; logs dessas métricas ficam no `metrics.json` lido por `read_transformation_metrics`, não no adapter.
- `classify_models` aceita formato de dict ao invés de objetos de domínio diretamente — o caller (Airflow callback) não tem acesso a importações do domain, e a conversão é responsabilidade do classifier como adapter de infraestrutura.

**Placeholder scan:** Nenhum step contém "TBD", "implement later", ou "similar to Task N".

**Type consistency:** `classify_models`, `submit_job`, `poll_job_status`, `cancel_job` — nomes e assinaturas consistentes em todas as Tasks.
