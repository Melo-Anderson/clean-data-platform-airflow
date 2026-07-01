# Data Platform — DataObject, DataElement & Pipeline v3 (DAG Generator + Airflow 3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Mudanças em relação à v1:**
- `DataObject` sem `pipeline_id` e sem `ObjectRole` (relação N:N via tabela associativa)
- `ScheduleConfig` sem default — campo obrigatório no `Pipeline`
- `PipelineDependency` com `DependencyType` enum para extensibilidade
- `SourceObjectConfig` separado em `SensorConfig` + `ExtractionConfig`
- `LineageMapping` domain para linhagem explícita a nível de coluna
- Airflow 3 (`airflow.sdk`, `Asset`, `@dag`, `@task`, `@task.sensor`, deferrable operators)
- Templates de DAG reestruturados (15 tasks ingestion, 14 export, 12 etl)
- Polimorfismo nas callbacks: `ComputeJobAdapter(Protocol)` + módulo `shared_callbacks.py`

**Mudanças v3 (este documento):**
- **Mandatory vs Optional tasks** classificação explícita por task (ver seção abaixo)
- **TaskGroup** para contextualizar tasks: `pre_flight`, `compute_engine`, `validation_and_metrics`, `data_load`, `observability`
- **`get_platform_client()`** factory com `@cache` substituindo `PlatformApiClient()` direto
- **Notificação síncrona** — removido `asyncio.run()` em `success_notification`
- **`PipelineRun`** entidade de domínio + ORM model para dashboard operacional (`last_run_at`, `last_success_at`, `status`, `failed_task`)

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy 2.x async, Pydantic v2, PyYAML, Jinja2, sqlglot, Apache Airflow 3.0+, uv, ruff, mypy.

## Global Constraints

- **Airflow 3**: `from airflow.sdk import Asset, dag, task, TaskGroup`. `@task` (TaskFlow). `@task.sensor` para sensors.
- **Mandatory task**: `trigger_rule` default (`all_success`). Falha bloqueia o DAG.
- **Optional task**: `@task(soft_fail=True)`. Falha registrada/alertada sem bloquear dados.
- **TaskGroup**: organização por contexto — `pre_flight`, `compute_engine`, `validation_and_metrics`, `data_load`, `observability`.
- **`get_platform_client()`**: factory com `@cache` em `infrastructure/platform_client.py`. Todos os callbacks importam via esta função — nunca instanciam `PlatformApiClient()` diretamente.
- **`PipelineRun`**: entidade persistida pelo `emit_monitoring_and_sla` (trigger_rule=all_done). Alimenta dashboard operacional com `last_run_at`, `last_success_at`, `status`, `failed_task`.
- **Sem `ObjectRole`** no DataObject — papel é pipeline-contextual.
- **DataObject:Pipeline = N:M** — sem `pipeline_id`, com `PipelineObjectModel`.
- **ScheduleConfig sem default** — obrigatório no cadastro do Pipeline.
- XCom carrega apenas referências externas — nunca dados volumosos.
- Code comments em inglês. Documentação `.md` em português.

---

## Task Classification — Mandatory vs Optional

### Critério
- **MANDATORY**: falha invalida o resultado prático dos dados. Downstream não deve receber dados incorretos ou incompletos. Falha propaga para o DAG.
- **OPTIONAL**: falha causada por sistema externo (plataforma de monitoração, catálogo, notificador) que não compromete a disponibilidade ou integridade dos dados. `soft_fail=True` — falha registrada mas não propaga.

### Ingestion

| Task | Tipo | Justificativa |
|---|---|---|
| `check_dependencies` | **MANDATORY** | Upstream incompleto → dados da execução seriam incorretos |
| `validate_source_and_discovery` | **MANDATORY** | Origem indisponível → nenhum dado extraído |
| `classify_changes_and_plan_actions` | **MANDATORY** | Schema crítico incompatível → dados corrompidos na carga |
| `source_readiness_sensor` | **MANDATORY** | Lote não pronto → extração de dados incompletos |
| `submit_compute_job` | **MANDATORY** | Sem job → sem dado |
| `monitor_compute_job` | **MANDATORY** | Sem monitoração → estado do job desconhecido |
| `validate_compute_execution` | **MANDATORY** | Job falhou → parquet corrompido ou ausente |
| `read_compute_metrics` | **OPTIONAL** | Storage pode estar lento; quality_gate tem fallback com métricas vazias |
| `quality_gate` | **MANDATORY** | Dados fora de spec não devem ser carregados no DW |
| `load_to_data_warehouse` | **MANDATORY** | Objetivo principal da ingestão |
| `post_load_validation` | **MANDATORY** | Volume e checksum divergentes indicam carga corrompida |
| `emit_raw_lineage` | **OPTIONAL** | Catálogo de linhagem pode estar indisponível |
| `emit_final_lineage` | **OPTIONAL** | Idem — falha não afeta dados no DW |
| `emit_monitoring_and_sla` | **OPTIONAL** | Ferramenta de monitoring pode estar down; `trigger_rule=all_done` |
| `success_notification` | **OPTIONAL** | Side effect — falha no notificador não invalida dados |

### ETL

| Task | Tipo | Justificativa |
|---|---|---|
| `check_dependencies` | **MANDATORY** | |
| `validate_source_models` | **MANDATORY** | Modelos dbt/Dataform ausentes → transformação quebrada |
| `classify_schema_changes` | **MANDATORY** | Schema crítico incompatível → resultado incorreto |
| `submit_transformation_job` | **MANDATORY** | |
| `monitor_transformation_job` | **MANDATORY** | |
| `validate_transformation_execution` | **MANDATORY** | |
| `read_transformation_metrics` | **OPTIONAL** | Storage pode estar lento |
| `quality_gate` | **MANDATORY** | |
| `publish_documentation` | **OPTIONAL** | Catálogo/dbt docs pode estar down |
| `emit_lineage` | **OPTIONAL** | |
| `emit_monitoring_and_sla` | **OPTIONAL** | `trigger_rule=all_done` |
| `success_notification` | **OPTIONAL** | |

### Export

| Task | Tipo | Justificativa |
|---|---|---|
| `check_dependencies` | **MANDATORY** | |
| `validate_export_configuration` | **MANDATORY** | Config inválida → entrega errada |
| `validate_source_dataset_readiness` | **MANDATORY** | Dado STALE → exportação de dado desatualizado |
| `classify_export_actions` | **MANDATORY** | |
| `submit_compute_export_job` | **MANDATORY** | |
| `monitor_compute_export_job` | **MANDATORY** | |
| `validate_compute_execution` | **MANDATORY** | |
| `read_export_metrics` | **OPTIONAL** | |
| `quality_gate` | **MANDATORY** | |
| `publish_export_artifacts` | **MANDATORY** | Objetivo principal da exportação |
| `validate_delivery` | **MANDATORY** | Entrega incompleta é falha crítica |
| `emit_export_lineage` | **OPTIONAL** | |
| `emit_monitoring_and_sla` | **OPTIONAL** | `trigger_rule=all_done` |
| `success_notification` | **OPTIONAL** | |

### TaskGroup Layout (ingestion como referência)

```
@dag
└── ingestion_dag
    ├── TaskGroup("pre_flight")               # MANDATORY
    │   ├── check_dependencies
    │   ├── validate_source_and_discovery
    │   └── classify_changes_and_plan_actions
    │
    ├── TaskGroup("source_readiness")         # MANDATORY — gerado apenas se sensor configurado
    │   └── source_readiness_sensor_*
    │
    ├── TaskGroup("compute_engine")           # MANDATORY
    │   ├── submit_compute_job
    │   ├── monitor_compute_job               # @task.sensor mode=reschedule
    │   └── validate_compute_execution
    │
    ├── TaskGroup("validation_and_metrics")
    │   ├── read_compute_metrics              # OPTIONAL soft_fail=True
    │   └── quality_gate                     # MANDATORY
    │
    ├── TaskGroup("data_load")                # MANDATORY
    │   ├── load_to_data_warehouse
    │   └── post_load_validation
    │
    └── TaskGroup("observability")            # all OPTIONAL soft_fail=True
        ├── emit_raw_lineage
        ├── emit_final_lineage
        ├── emit_monitoring_and_sla           # trigger_rule=all_done → persiste PipelineRun
        └── success_notification
```

---

## Estrutura de Arquivos

```
platform/
├── domain/
│   ├── objects/
│   │   ├── object_type.py                   # ObjectType(StrEnum): TABLE, VIEW, FILE, API_RESOURCE, COLLECTION
│   │   ├── freshness_status.py              # FreshnessStatus(StrEnum): FRESH, STALE, UNKNOWN
│   │   ├── element_type.py                  # ElementType(StrEnum): STRING, INTEGER, ...
│   │   ├── data_element.py                  # DataElement entity
│   │   ├── data_object.py                   # DataObject entity (sem pipeline_id, sem role)
│   │   ├── object_repository.py             # DataObjectRepository Protocol
│   │   └── object_service.py                # DataObjectService
│   ├── pipelines/
│   │   ├── pipeline_type.py                 # PipelineType: ingestion | etl | export
│   │   ├── schedule_mode.py                 # ScheduleMode: cron | trigger | trigger_with_gate
│   │   ├── dependency_type.py               # DependencyType: dataset | external_event | manual
│   │   ├── load_strategy.py                 # LoadStrategy: full_load | incremental | cdc
│   │   ├── transform_engine.py              # TransformEngine: dbt | dataform | none
│   │   ├── compute_engine.py                # ComputeEngine: spark | dataflow | default
│   │   ├── on_critical_change.py            # OnCriticalChange: block | self_heal | alert_only
│   │   ├── quality_rule_type.py             # QualityRuleType: not_null | row_count_min | unique | ...
│   │   ├── pipeline_run_status.py           # [NEW] PipelineRunStatus: running|success|failed|quality_failed|partial
│   │   ├── pipeline_dependency.py           # PipelineDependency(pipeline_id, require_same_day, dependency_type)
│   │   ├── schedule_config.py               # ScheduleConfig Value Object (sem default no Pipeline)
│   │   ├── sensor_config.py                 # SensorConfig Value Object
│   │   ├── extraction_config.py             # ExtractionConfig Value Object
│   │   ├── destination_object_config.py     # DestinationObjectConfig Value Object
│   │   ├── transform_config.py              # TransformConfig Value Object
│   │   ├── compute_config.py                # ComputeConfig Value Object
│   │   ├── quality_rule.py                  # QualityRule Value Object
│   │   ├── airflow_config.py                # AirflowConfig Value Object
│   │   ├── discovery_task_config.py         # DiscoveryTaskConfig Value Object
│   │   ├── pipeline.py                      # Pipeline entity — schedule sem default
│   │   ├── pipeline_run.py                  # [NEW] PipelineRun entity (dashboard operacional)
│   │   ├── pipeline_repository.py           # PipelineRepository Protocol
│   │   ├── pipeline_run_repository.py       # [NEW] PipelineRunRepository Protocol
│   │   └── pipeline_service.py              # PipelineService
│   └── lineage/
│       ├── lineage_mapping.py               # LineageMapping entity + ColumnLineage Value Object
│       └── lineage_repository.py            # LineageRepository Protocol
│
├── application/
│   ├── unit_of_work.py                      # [UPDATED] + pipeline_runs, lineage
│   └── pipelines/
│       ├── register_pipeline.py
│       └── rebuild_pipelines.py
│
└── infrastructure/
    ├── platform_client.py                   # [NEW] get_platform_client() factory @cache
    ├── persistence/
    │   ├── models/
    │   │   ├── data_object_model.py         # sem pipeline_id, sem role
    │   │   ├── data_element_model.py
    │   │   ├── pipeline_model.py
    │   │   ├── pipeline_run_model.py        # [NEW] last_run_at, last_success_at, status, failed_task
    │   │   ├── pipeline_object_model.py     # tabela associativa Pipeline ↔ DataObject
    │   │   └── lineage_mapping_model.py     # LineageMapping ORM
    │   └── repositories/
    │       ├── sql_object_repository.py
    │       ├── sql_pipeline_repository.py
    │       ├── sql_pipeline_run_repository.py  # [NEW]
    │       └── sql_lineage_repository.py
    ├── airflow_callbacks/
    │   ├── shared_callbacks.py              # funções comuns (check_deps, validate_exec, quality_gate,
    │   │                                    #   emit_monitoring sync, success_notification sync)
    │   ├── ingestion_callbacks.py           # funções específicas de ingestion
    │   ├── export_callbacks.py              # funções específicas de export
    │   ├── etl_callbacks.py                 # funções específicas de ETL
    │   └── compute_job_adapter.py           # ComputeJobAdapter(Protocol)
    ├── yaml_generator/
    │   ├── pipeline_yaml_generator.py
    │   ├── yaml_schema_validator.py
    │   └── git_committer.py
    └── dag_generator/
        ├── dag_generator.py
        ├── schema_migrator.py
        ├── ci_validator.py
        └── templates/
            ├── _shared_macros.j2            # macros Jinja2: dag_imports, pipeline_asset, schedule, default_args
            ├── ingestion_dag.py.j2          # 15 tasks + 6 TaskGroups
            ├── etl_dag.py.j2                # 12 tasks + 4 TaskGroups
            └── export_dag.py.j2             # 14 tasks + 5 TaskGroups
```

---

## Task 1: Domain — DataObject e DataElement (sem ObjectRole, sem pipeline_id)

**Mudanças em relação à v1:**
- Remove `role: ObjectRole` do `DataObject`
- Remove `pipeline_id` do `DataObject`
- Sem enum `ObjectRole`
- Relação DataObject ↔ Pipeline via `PipelineObjectModel` (Task 3)

---

- [ ] **Step 1: Criar enums de domínio**

```python
# platform/domain/objects/object_type.py
from __future__ import annotations
from enum import StrEnum

class ObjectType(StrEnum):
    """Supported DataObject structural types."""
    TABLE = "table"
    VIEW = "view"
    FILE = "file"
    API_RESOURCE = "api_resource"
    COLLECTION = "collection"
```

```python
# platform/domain/objects/freshness_status.py
from __future__ import annotations
from enum import StrEnum

class FreshnessStatus(StrEnum):
    FRESH = "fresh"     # last_success within expected schedule window
    STALE = "stale"     # last_success outside expected schedule window
    UNKNOWN = "unknown" # no execution recorded yet
```

```python
# platform/domain/objects/element_type.py
from __future__ import annotations
from enum import StrEnum

class ElementType(StrEnum):
    """Supported data types for DataElement source and destination mapping."""
    STRING = "string"
    INTEGER = "integer"
    BIGINT = "bigint"
    FLOAT = "float"
    DECIMAL = "decimal"
    DATE = "date"
    TIMESTAMP = "timestamp"
    BOOLEAN = "boolean"
    BYTES = "bytes"
    JSON = "json"
```

- [ ] **Step 2: Criar DataElement**

```python
# platform/domain/objects/data_element.py
from __future__ import annotations

from dataclasses import dataclass

from platform.domain.objects.element_type import ElementType
from platform.domain.shared.auditable import Auditable
from platform.domain.shared.policy_tag import PolicyTag


@dataclass
class DataElement(Auditable):
    """
    A single field or attribute within a DataObject.

    source_type is immutable after Discovery — changing it would break lineage traceability.
    destination_type, required, and nullable can be overridden by the Analytics Engineer
    to enforce schema contracts at the destination independent of the source.

    is_computed=True for ETL-calculated fields with no source counterpart (no source_type).
    auto_generated=True when description or policy_tag was inferred by Discovery, not confirmed.
    """

    id: str
    object_id: str
    name: str
    source_type: ElementType | None  # None for is_computed=True fields
    destination_type: ElementType
    required: bool = False
    nullable: bool = True
    description: str = ""
    policy_tag: PolicyTag | None = None
    auto_generated: bool = False
    is_computed: bool = False
```

- [ ] **Step 3: Criar DataObject (sem pipeline_id, sem role)**

```python
# platform/domain/objects/data_object.py
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from platform.domain.objects.data_element import DataElement
from platform.domain.objects.freshness_status import FreshnessStatus
from platform.domain.objects.object_type import ObjectType
from platform.domain.shared.auditable import Auditable
from platform.domain.shared.policy_tag import PolicyTag


@dataclass
class DataObject(Auditable):
    """
    Logical data entity within a DataAsset (table, file, API resource, view, or collection).

    Intentionally has no pipeline_id: a DataObject can participate in multiple pipelines
    (as source in one ingestion, as destination in another ETL, etc.).
    The relationship DataObject ↔ Pipeline is managed by PipelineObjectRef (infrastructure).

    freshness_status is calculated by the catalog layer based on last_success vs schedule.
    policy_tags are inherited from the parent DataAsset and refined per DataElement.
    """

    id: str
    asset_id: str
    name: str
    type: ObjectType
    description: str = ""
    policy_tags: list[PolicyTag] = field(default_factory=list)
    last_run: datetime | None = None
    last_success: datetime | None = None
    freshness_status: FreshnessStatus = FreshnessStatus.UNKNOWN
    elements: list[DataElement] = field(default_factory=list)
    auto_generated_description: bool = False
```

- [ ] **Step 4: Criar DataObjectRepository Protocol e DataObjectService**

```python
# platform/domain/objects/object_repository.py
from __future__ import annotations
from typing import Protocol, runtime_checkable
from platform.domain.objects.data_element import DataElement
from platform.domain.objects.data_object import DataObject


@runtime_checkable
class DataObjectRepository(Protocol):
    async def save(self, obj: DataObject) -> DataObject: ...
    async def find_by_id(self, object_id: str) -> DataObject | None: ...
    async def find_by_asset_id(self, asset_id: str) -> list[DataObject]: ...
    async def add_element(self, object_id: str, element: DataElement) -> DataElement: ...
    async def update_element_destination_type(
        self, element_id: str, destination_type: str, required: bool, nullable: bool
    ) -> DataElement: ...
```

```python
# platform/domain/objects/object_service.py
from __future__ import annotations

from platform.domain.objects.data_element import DataElement
from platform.domain.objects.data_object import DataObject
from platform.domain.objects.element_type import ElementType
from platform.domain.objects.object_repository import DataObjectRepository
from platform.domain.objects.object_type import ObjectType
from platform.domain.shared.policy_tag import PolicyTag

# Type pairs where destination_type override is considered destructive (lossy cast)
_DESTRUCTIVE_OVERRIDES: frozenset[tuple[ElementType, ElementType]] = frozenset({
    (ElementType.STRING, ElementType.INTEGER),
    (ElementType.STRING, ElementType.BIGINT),
    (ElementType.STRING, ElementType.FLOAT),
    (ElementType.STRING, ElementType.DATE),
    (ElementType.STRING, ElementType.TIMESTAMP),
    (ElementType.STRING, ElementType.BOOLEAN),
})


class ObjectNotFoundError(Exception):
    def __init__(self, object_id: str) -> None:
        super().__init__(f"DataObject not found: id={object_id!r}")
        self.object_id = object_id


class DestructiveOverrideWarning(Exception):
    """
    Raised when destination_type is type-incompatible with source_type.

    This is a warning-level validation: CI flags it and blocks deploy
    until the Analytics Engineer explicitly confirms the override.
    """

    def __init__(self, element_name: str, source: ElementType, destination: ElementType) -> None:
        super().__init__(
            f"Destructive type override on element '{element_name}': "
            f"source_type={source!r} → destination_type={destination!r}. "
            "Explicit AE confirmation required before deploy."
        )


class DataObjectService:
    """
    Domain service for DataObject and DataElement management.
    No FastAPI. No SQLAlchemy. Depends only on DataObjectRepository Protocol.
    """

    def __init__(self, repo: DataObjectRepository) -> None:
        self._repo = repo

    async def register(
        self,
        object_id: str,
        asset_id: str,
        name: str,
        object_type: ObjectType,
        description: str,
        policy_tags: list[PolicyTag],
    ) -> DataObject:
        obj = DataObject(
            id=object_id,
            asset_id=asset_id,
            name=name,
            type=object_type,
            description=description,
            policy_tags=policy_tags,
        )
        return await self._repo.save(obj)

    async def add_element(self, object_id: str, element: DataElement) -> DataElement:
        await self._require_object(object_id)
        return await self._repo.add_element(object_id, element)

    async def override_element_destination(
        self,
        object_id: str,
        element_id: str,
        element_name: str,
        source_type: ElementType | None,
        destination_type: ElementType,
        required: bool,
        nullable: bool,
    ) -> DataElement:
        """
        Override destination_type for a DataElement.

        Raises DestructiveOverrideWarning if source→destination is a known lossy cast.
        The caller (router or CI validator) decides whether to allow or reject.
        """
        await self._require_object(object_id)
        if source_type is not None and (source_type, destination_type) in _DESTRUCTIVE_OVERRIDES:
            raise DestructiveOverrideWarning(element_name, source_type, destination_type)
        return await self._repo.update_element_destination_type(
            element_id, destination_type.value, required, nullable
        )

    async def _require_object(self, object_id: str) -> DataObject:
        obj = await self._repo.find_by_id(object_id)
        if obj is None:
            raise ObjectNotFoundError(object_id)
        return obj
```

- [ ] **Step 5: Escrever testes unitários**

```python
# tests/unit/domain/objects/test_object_service.py
from __future__ import annotations

import uuid
import pytest

from platform.domain.objects.data_element import DataElement
from platform.domain.objects.data_object import DataObject
from platform.domain.objects.element_type import ElementType
from platform.domain.objects.object_service import DataObjectService, DestructiveOverrideWarning, ObjectNotFoundError
from platform.domain.objects.object_type import ObjectType
from platform.domain.shared.policy_tag import PolicyTag


class FakeDataObjectRepository:
    def __init__(self) -> None:
        self._objects: dict[str, DataObject] = {}
        self._elements: dict[str, DataElement] = {}

    async def save(self, obj: DataObject) -> DataObject:
        self._objects[obj.id] = obj
        return obj

    async def find_by_id(self, object_id: str) -> DataObject | None:
        return self._objects.get(object_id)

    async def find_by_asset_id(self, asset_id: str) -> list[DataObject]:
        return [o for o in self._objects.values() if o.asset_id == asset_id]

    async def add_element(self, object_id: str, element: DataElement) -> DataElement:
        self._elements[element.id] = element
        self._objects[object_id].elements.append(element)
        return element

    async def update_element_destination_type(
        self, element_id: str, destination_type: str, required: bool, nullable: bool
    ) -> DataElement:
        el = self._elements[element_id]
        el.destination_type = ElementType(destination_type)
        el.required = required
        el.nullable = nullable
        return el


def _service() -> tuple[DataObjectService, FakeDataObjectRepository]:
    repo = FakeDataObjectRepository()
    return DataObjectService(repo=repo), repo


@pytest.mark.asyncio
async def test_register_creates_data_object_without_role_or_pipeline() -> None:
    service, _ = _service()
    obj = await service.register(
        object_id=str(uuid.uuid4()),
        asset_id="asset-1",
        name="customers",
        object_type=ObjectType.TABLE,
        description="Customer data",
        policy_tags=[PolicyTag.PII],
    )
    assert obj.name == "customers"
    assert not hasattr(obj, "role"), "DataObject must not have an ObjectRole attribute"
    assert not hasattr(obj, "pipeline_id"), "DataObject must not have a pipeline_id attribute"


@pytest.mark.asyncio
async def test_destructive_override_raises_warning() -> None:
    service, _ = _service()
    obj = await service.register(str(uuid.uuid4()), "a1", "t", ObjectType.TABLE, "", [])
    el = DataElement(
        id=str(uuid.uuid4()), object_id=obj.id, name="zip",
        source_type=ElementType.STRING, destination_type=ElementType.STRING,
    )
    await service.add_element(obj.id, el)
    with pytest.raises(DestructiveOverrideWarning):
        await service.override_element_destination(obj.id, el.id, "zip", ElementType.STRING, ElementType.INTEGER, False, True)


@pytest.mark.asyncio
async def test_integer_to_bigint_is_not_destructive() -> None:
    service, _ = _service()
    obj = await service.register(str(uuid.uuid4()), "a1", "t", ObjectType.TABLE, "", [])
    el = DataElement(
        id=str(uuid.uuid4()), object_id=obj.id, name="age",
        source_type=ElementType.INTEGER, destination_type=ElementType.INTEGER,
    )
    await service.add_element(obj.id, el)
    updated = await service.override_element_destination(obj.id, el.id, "age", ElementType.INTEGER, ElementType.BIGINT, True, False)
    assert updated.destination_type == ElementType.BIGINT
```

- [ ] **Step 6: Rodar testes, commit**

```bash
uv run pytest tests/unit/domain/objects/ -v
git add platform/domain/objects/ tests/unit/domain/objects/
git commit -m "feat: add DataObject (no role, no pipeline_id), DataElement with destructive override detection"
```

---

## Task 2: Domain — Pipeline Value Objects (ScheduleConfig obrigatório, SensorConfig separado, DependencyType)

**Mudanças em relação à v1:**
- `DependencyType` enum novo
- `PipelineDependency` com `dependency_type`
- `ScheduleConfig` sem default no `Pipeline` entity
- `SourceObjectConfig` dividido em `SensorConfig` + `ExtractionConfig`

---

- [ ] **Step 1: Criar enums de pipeline**

```python
# platform/domain/pipelines/pipeline_type.py
from enum import StrEnum
class PipelineType(StrEnum):
    INGESTION = "ingestion"
    ETL = "etl"
    EXPORT = "export"

# platform/domain/pipelines/schedule_mode.py
from enum import StrEnum
class ScheduleMode(StrEnum):
    CRON = "cron"
    TRIGGER = "trigger"
    TRIGGER_WITH_GATE = "trigger_with_gate"

# platform/domain/pipelines/dependency_type.py
from __future__ import annotations
from enum import StrEnum

class DependencyType(StrEnum):
    """
    How this pipeline depends on upstream work.

    DATASET: Triggered by Airflow Asset event when upstream DAG completes.
      Implemented via schedule=[Asset("platform://pipeline/{id}")].
    EXTERNAL_EVENT: Triggered by an external webhook or event bus.
      Future: EventBridge, Pub/Sub, custom sensor. Reserved for extensibility.
    MANUAL: Requires explicit human trigger. Not automated.
      Implemented via schedule=None with manual trigger only.
    """
    DATASET = "dataset"          # Airflow Asset-based scheduling (current implementation)
    EXTERNAL_EVENT = "external_event"  # Future: external event bus integration
    MANUAL = "manual"            # Future: manual trigger gates

# platform/domain/pipelines/load_strategy.py
from enum import StrEnum
class LoadStrategy(StrEnum):
    FULL_LOAD = "full_load"
    INCREMENTAL = "incremental"
    CDC = "cdc"

# platform/domain/pipelines/transform_engine.py
from enum import StrEnum
class TransformEngine(StrEnum):
    DBT = "dbt"
    DATAFORM = "dataform"
    NONE = "none"

# platform/domain/pipelines/compute_engine.py
from enum import StrEnum
class ComputeEngine(StrEnum):
    SPARK = "spark"
    DATAFLOW = "dataflow"
    DEFAULT = "default"

# platform/domain/pipelines/on_critical_change.py
from enum import StrEnum
class OnCriticalChange(StrEnum):
    BLOCK = "block"
    SELF_HEAL = "self_heal"
    ALERT_ONLY = "alert_only"

# platform/domain/pipelines/quality_rule_type.py
from enum import StrEnum
class QualityRuleType(StrEnum):
    NOT_NULL = "not_null"
    ROW_COUNT_MIN = "row_count_min"
    UNIQUE = "unique"
    ACCEPTED_VALUES = "accepted_values"
    REFERENTIAL_INTEGRITY = "referential_integrity"
    CHECKSUM = "checksum"
```

- [ ] **Step 2: Criar PipelineDependency com DependencyType**

```python
# platform/domain/pipelines/pipeline_dependency.py
from __future__ import annotations
from dataclasses import dataclass
from platform.domain.pipelines.dependency_type import DependencyType


@dataclass(frozen=True)
class PipelineDependency:
    """
    Represents a dependency of this pipeline on an upstream pipeline or event.

    dependency_type determines how the dependency is implemented in Airflow:
      DATASET → schedule=[Asset("platform://pipeline/{pipeline_id}")]
      EXTERNAL_EVENT → reserved for future event-based triggers
      MANUAL → reserved for manual gate tasks

    require_same_day is only meaningful for DATASET dependencies:
      True → ShortCircuitOperator gate enforces same calendar day (UTC) success.
    """

    pipeline_id: str
    require_same_day: bool = False
    dependency_type: DependencyType = DependencyType.DATASET
```

- [ ] **Step 3: Criar ScheduleConfig sem default no Pipeline**

```python
# platform/domain/pipelines/schedule_config.py
from __future__ import annotations
from dataclasses import dataclass, field
from platform.domain.pipelines.schedule_mode import ScheduleMode
from platform.domain.pipelines.pipeline_dependency import PipelineDependency
from platform.domain.shared.value_objects import CronSchedule


@dataclass(frozen=True)
class ScheduleConfig:
    """
    Scheduling configuration for a Pipeline.

    Always required — no default in Pipeline. Caller must explicitly declare schedule intent.

    mode=cron: cron_schedule required. No depends_on.
    mode=trigger: depends_on required (DATASET type). cron_schedule ignored.
    mode=trigger_with_gate: both cron_schedule and depends_on required.
      Airflow 3: schedule=[Asset(...)], plus ShortCircuitOperator enforcing require_same_day.

    Only DependencyType.DATASET is implemented in this plan version.
    EXTERNAL_EVENT and MANUAL are reserved for future plans.
    """

    mode: ScheduleMode
    cron_schedule: CronSchedule | None = None
    depends_on: tuple[PipelineDependency, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.mode == ScheduleMode.CRON and self.cron_schedule is None:
            raise ValueError("ScheduleConfig(mode='cron') requires cron_schedule")
        if self.mode == ScheduleMode.TRIGGER and not self.depends_on:
            raise ValueError("ScheduleConfig(mode='trigger') requires at least one depends_on entry")
        if self.mode == ScheduleMode.TRIGGER_WITH_GATE:
            if self.cron_schedule is None:
                raise ValueError("ScheduleConfig(mode='trigger_with_gate') requires cron_schedule")
            if not self.depends_on:
                raise ValueError("ScheduleConfig(mode='trigger_with_gate') requires at least one depends_on entry")
```

- [ ] **Step 4: Criar SensorConfig e ExtractionConfig separados**

```python
# platform/domain/pipelines/sensor_config.py
from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class SensorConfig:
    """
    Pre-extraction readiness sensor configuration.

    Encapsulates all sensor-related settings independently of extraction settings.
    Separation of concern: sensor logic is about 'can I start?' while
    ExtractionConfig is about 'how do I extract?'.

    Implemented in Airflow 3 via @task.sensor with mode="reschedule".

    query: SQL that returns a truthy value (1, TRUE, non-empty) when the source is ready.
    timeout_minutes: sensor fails if the query does not return truthy within this window.
    poke_interval_seconds: interval between query re-executions. Use reschedule mode
      to free worker slots between pokes.
    """

    query: str
    timeout_minutes: int = 60
    poke_interval_seconds: int = 60

    def __post_init__(self) -> None:
        if not self.query.strip():
            raise ValueError("SensorConfig.query cannot be empty")
        if self.timeout_minutes <= 0:
            raise ValueError("SensorConfig.timeout_minutes must be > 0")
        if self.poke_interval_seconds <= 0:
            raise ValueError("SensorConfig.poke_interval_seconds must be > 0")
```

```python
# platform/domain/pipelines/extraction_config.py
from __future__ import annotations
from dataclasses import dataclass, field
from platform.domain.pipelines.load_strategy import LoadStrategy
from platform.domain.pipelines.sensor_config import SensorConfig


@dataclass(frozen=True)
class ExtractionConfig:
    """
    Extraction configuration for one source DataObject within a Pipeline.

    Replaces SourceObjectConfig from v1, with sensor settings extracted to SensorConfig.

    extraction_query: When provided, replaces the auto-generated query.
      AE must include the watermark filter manually when using incremental load.
    sensor: When provided, a readiness sensor task is generated before extraction.
      None means no sensor — extraction starts immediately after classify_changes_and_plan_actions.

    XCom policy: the extract task writes data to GCS/S3 and passes only the path via XCom.
    """

    object_id: str
    load_strategy: LoadStrategy = LoadStrategy.FULL_LOAD
    watermark_column: str | None = None
    page_size: int = 1000
    partition_column: str | None = None
    compression: str = "snappy"
    encoding: str = "utf-8"
    extraction_query: str | None = None
    sensor: SensorConfig | None = None

    def has_sensor(self) -> bool:
        return self.sensor is not None
```

- [ ] **Step 5: Criar demais Value Objects e Pipeline entity (schedule sem default)**

```python
# platform/domain/pipelines/destination_object_config.py
from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class DestinationObjectConfig:
    object_id: str
    create_if_not_exists: bool = True
```

```python
# platform/domain/pipelines/transform_config.py
from __future__ import annotations
from dataclasses import dataclass
from platform.domain.pipelines.transform_engine import TransformEngine

@dataclass(frozen=True)
class TransformConfig:
    """
    engine=dbt: submit_transformation_job runs `dbt run --select {ref}`.
    engine=dataform: uses DataformCreateWorkflowInvocationOperator (native GCP).
    engine=none: no transformation job — object passes through unchanged.
    """
    engine: TransformEngine = TransformEngine.NONE
    ref: str | None = None

    def __post_init__(self) -> None:
        if self.engine != TransformEngine.NONE and not self.ref:
            raise ValueError(f"TransformConfig(engine={self.engine!r}) requires ref")
```

```python
# platform/domain/pipelines/compute_config.py
from __future__ import annotations
from dataclasses import dataclass
from platform.domain.pipelines.compute_engine import ComputeEngine

@dataclass(frozen=True)
class ComputeConfig:
    """
    Compute engine for the extraction/transformation job.

    The compute engine is responsible for:
    extract → canonicalize → cast_technical_types → add_processing_timestamp
    → basic_quality_validations → write_parquet → write_schema_json → write_metrics_json
    """
    engine: ComputeEngine = ComputeEngine.DEFAULT
    num_workers: int = 1
    machine_type: str = "n1-standard-2"
    staging_bucket: str = ""  # GCS/S3 bucket for parquet output
```

```python
# platform/domain/pipelines/quality_rule.py
from __future__ import annotations
from dataclasses import dataclass
from platform.domain.pipelines.quality_rule_type import QualityRuleType

@dataclass(frozen=True)
class QualityRule:
    """
    Quality assertion applied by quality_gate after read_compute_metrics.

    quality_gate confronts configured rules with metrics produced by the compute engine
    (written to metrics.json). Failure marks task as quality_failed, blocking downstream.
    """
    type: QualityRuleType
    column: str | None = None
    value: int | float | None = None
```

```python
# platform/domain/pipelines/airflow_config.py
from __future__ import annotations
from dataclasses import dataclass, field

@dataclass(frozen=True)
class AirflowConfig:
    """
    Airflow 3 DAG-level configuration.

    sla_minutes: emit_monitoring_and_sla fires a SLA breach alert if the DAG does not
      complete within this window from scheduled time.
    pool: Airflow worker pool. Controls resource concurrency per group.
    """
    retries: int = 3
    retry_delay_minutes: int = 5
    execution_timeout_minutes: int = 120
    sla_minutes: int = 90
    tags: tuple[str, ...] = field(default_factory=tuple)
    pool: str = "default_pool"
```

```python
# platform/domain/pipelines/discovery_task_config.py
from __future__ import annotations
from dataclasses import dataclass
from platform.domain.pipelines.on_critical_change import OnCriticalChange

@dataclass(frozen=True)
class DiscoveryTaskConfig:
    """
    Controls validate_source_and_discovery + classify_changes_and_plan_actions tasks.

    on_critical_change applies to informative changes only.
    CRITICAL changes (nullable→required, incompatible type, table removed) always block.
    """
    enabled: bool = True
    on_critical_change: OnCriticalChange = OnCriticalChange.BLOCK
```

```python
# platform/domain/pipelines/pipeline.py
from __future__ import annotations

from dataclasses import dataclass, field

from platform.domain.pipelines.airflow_config import AirflowConfig
from platform.domain.pipelines.compute_config import ComputeConfig
from platform.domain.pipelines.destination_object_config import DestinationObjectConfig
from platform.domain.pipelines.discovery_task_config import DiscoveryTaskConfig
from platform.domain.pipelines.extraction_config import ExtractionConfig
from platform.domain.pipelines.pipeline_type import PipelineType
from platform.domain.pipelines.quality_rule import QualityRule
from platform.domain.pipelines.schedule_config import ScheduleConfig
from platform.domain.pipelines.transform_config import TransformConfig
from platform.domain.shared.auditable import Auditable
from platform.domain.shared.value_objects import EmailAddress

CURRENT_SCHEMA_VERSION = "1.0"


@dataclass
class Pipeline(Auditable):
    """
    Pipeline aggregate. Represents a DAG in Airflow.

    schedule has no default — must be explicitly declared at construction.
    This prevents accidental pipelines with unspecified scheduling intent.

    dataset_uri: Airflow 3 Asset URI for this pipeline.
      Used as DAG outlet (what this pipeline produces) and as inlet in downstream deps.
      Convention: platform://pipeline/{id}
    """

    id: str
    name: str
    type: PipelineType
    owner: EmailAddress
    schedule: ScheduleConfig  # Required — no default. Caller must declare scheduling intent.
    schema_version: str = CURRENT_SCHEMA_VERSION
    source_asset_id: str = ""
    source_objects: list[ExtractionConfig] = field(default_factory=list)
    destination_asset_id: str = ""
    destination_objects: list[DestinationObjectConfig] = field(default_factory=list)
    transform: TransformConfig = field(default_factory=TransformConfig)
    compute: ComputeConfig = field(default_factory=ComputeConfig)
    quality_rules: list[QualityRule] = field(default_factory=list)
    airflow: AirflowConfig = field(default_factory=AirflowConfig)
    discovery_task: DiscoveryTaskConfig = field(default_factory=DiscoveryTaskConfig)

    @property
    def dataset_uri(self) -> str:
        """Airflow 3 Asset URI. Declared as outlet on every generated DAG."""
        return f"platform://pipeline/{self.id}"
```

- [ ] **Step 6: Escrever testes**

```python
# tests/unit/domain/pipelines/test_schedule_config.py
from __future__ import annotations
import pytest
from platform.domain.pipelines.dependency_type import DependencyType
from platform.domain.pipelines.pipeline_dependency import PipelineDependency
from platform.domain.pipelines.schedule_config import ScheduleConfig
from platform.domain.pipelines.schedule_mode import ScheduleMode
from platform.domain.shared.value_objects import CronSchedule


def test_cron_without_expression_raises() -> None:
    with pytest.raises(ValueError, match="requires cron_schedule"):
        ScheduleConfig(mode=ScheduleMode.CRON)


def test_trigger_without_depends_on_raises() -> None:
    with pytest.raises(ValueError, match="requires at least one depends_on"):
        ScheduleConfig(mode=ScheduleMode.TRIGGER)


def test_trigger_with_gate_without_cron_raises() -> None:
    dep = PipelineDependency(pipeline_id="uuid-1", require_same_day=True)
    with pytest.raises(ValueError, match="requires cron_schedule"):
        ScheduleConfig(mode=ScheduleMode.TRIGGER_WITH_GATE, depends_on=(dep,))


def test_dependency_type_defaults_to_dataset() -> None:
    dep = PipelineDependency(pipeline_id="uuid-1")
    assert dep.dependency_type == DependencyType.DATASET


def test_external_event_dependency_type_can_be_declared() -> None:
    dep = PipelineDependency(
        pipeline_id="uuid-2",
        dependency_type=DependencyType.EXTERNAL_EVENT,
        require_same_day=False,
    )
    assert dep.dependency_type == DependencyType.EXTERNAL_EVENT


# tests/unit/domain/pipelines/test_sensor_config.py
from __future__ import annotations
import pytest
from platform.domain.pipelines.sensor_config import SensorConfig


def test_valid_sensor_config_creates_instance() -> None:
    sensor = SensorConfig(query="SELECT 1 FROM batch WHERE done=1", timeout_minutes=60)
    assert sensor.has_sensor_query()


def test_empty_query_raises() -> None:
    with pytest.raises(ValueError, match="cannot be empty"):
        SensorConfig(query="   ")


def test_zero_timeout_raises() -> None:
    with pytest.raises(ValueError, match="must be > 0"):
        SensorConfig(query="SELECT 1", timeout_minutes=0)


# tests/unit/domain/pipelines/test_pipeline_service.py
from __future__ import annotations
import uuid
import pytest
from platform.domain.pipelines.airflow_config import AirflowConfig
from platform.domain.pipelines.extraction_config import ExtractionConfig
from platform.domain.pipelines.load_strategy import LoadStrategy
from platform.domain.pipelines.pipeline import Pipeline
from platform.domain.pipelines.pipeline_service import PipelineService
from platform.domain.pipelines.pipeline_type import PipelineType
from platform.domain.pipelines.schedule_config import ScheduleConfig
from platform.domain.pipelines.schedule_mode import ScheduleMode
from platform.domain.pipelines.sensor_config import SensorConfig
from platform.domain.shared.value_objects import CronSchedule, EmailAddress


class FakePipelineRepository:
    def __init__(self) -> None:
        self._store: dict[str, Pipeline] = {}
    async def save(self, p: Pipeline) -> Pipeline:
        self._store[p.id] = p
        return p
    async def find_by_id(self, pid: str) -> Pipeline | None:
        return self._store.get(pid)
    async def find_all(self) -> list[Pipeline]:
        return list(self._store.values())
    async def update_schema_version(self, pid: str, sv: str) -> Pipeline:
        self._store[pid].schema_version = sv
        return self._store[pid]


def _pipeline(**kwargs) -> Pipeline:
    defaults = dict(
        id=str(uuid.uuid4()),
        name="test",
        type=PipelineType.INGESTION,
        owner=EmailAddress("ae@co.com"),
        schedule=ScheduleConfig(mode=ScheduleMode.CRON, cron_schedule=CronSchedule("0 6 * * *")),
    )
    return Pipeline(**{**defaults, **kwargs})


@pytest.mark.asyncio
async def test_pipeline_schedule_is_required() -> None:
    """Pipeline must be constructed with an explicit schedule — no default."""
    with pytest.raises(TypeError):
        Pipeline(id="x", name="x", type=PipelineType.INGESTION, owner=EmailAddress("ae@co.com"))


@pytest.mark.asyncio
async def test_sensor_timeout_exceeding_execution_timeout_raises() -> None:
    service = PipelineService(repo=FakePipelineRepository())
    sensor = SensorConfig(query="SELECT 1", timeout_minutes=200)
    extraction = ExtractionConfig(object_id="obj-1", load_strategy=LoadStrategy.INCREMENTAL, sensor=sensor)
    pipeline = _pipeline(source_objects=[extraction], airflow=AirflowConfig(execution_timeout_minutes=120))
    with pytest.raises(ValueError, match="sensor.*timeout"):
        await service.register(pipeline)


@pytest.mark.asyncio
async def test_dataset_uri_follows_convention() -> None:
    service = PipelineService(repo=FakePipelineRepository())
    p = _pipeline()
    saved = await service.register(p)
    assert saved.dataset_uri == f"platform://pipeline/{saved.id}"
```

- [ ] **Step 7: Rodar testes, commit**

```bash
uv run pytest tests/unit/domain/pipelines/ -v
git add platform/domain/pipelines/ tests/unit/domain/pipelines/
git commit -m "feat: add Pipeline domain with SensorConfig/ExtractionConfig separation, DependencyType enum, required ScheduleConfig"
```

---

## Task 3: Domain — LineageMapping (Bounded Context de Linhagem)

**Novo bounded context para linhagem explícita a nível de coluna.**

---

- [ ] **Step 1: Criar LineageMapping e ColumnLineage**

```python
# platform/domain/lineage/lineage_mapping.py
from __future__ import annotations

from dataclasses import dataclass, field

from platform.domain.shared.auditable import Auditable


@dataclass(frozen=True)
class ColumnLineage:
    """
    Explicit lineage mapping between a source and destination column.

    transformation_expression: SQL expression or natural language describing
      how source_column is transformed to produce destination_column.
      None means direct copy (no transformation).

    Example:
        ColumnLineage(
            source_column="cpf",
            destination_column="document_hash",
            transformation_expression="SHA256(cpf)",
        )
    """

    source_column: str
    destination_column: str
    transformation_expression: str | None = None


@dataclass
class LineageMapping(Auditable):
    """
    Explicit column-level lineage between source and destination DataObjects within a Pipeline.

    Auto-populated by emit_raw_lineage task for ingestion (direct column mapping).
    Updated by emit_final_lineage / emit_lineage tasks for ETL/export (with transformations).

    Enables impact analysis: given a destination_column, trace back to source_column
    and all intermediate transformation_expressions across the pipeline chain.

    This entity is intentionally simple and evolvable:
    - Phase 1 (this plan): column mappings declared manually or inferred from schema matching.
    - Phase 2 (future): auto-generated from dbt graph or Dataform lineage API.
    - Phase 3 (future): cross-pipeline lineage graph traversal.
    """

    id: str
    pipeline_id: str
    source_object_id: str
    destination_object_id: str
    column_mappings: list[ColumnLineage] = field(default_factory=list)

    def add_mapping(
        self,
        source_column: str,
        destination_column: str,
        transformation_expression: str | None = None,
    ) -> ColumnLineage:
        """Add a column-level mapping and return the created ColumnLineage."""
        mapping = ColumnLineage(
            source_column=source_column,
            destination_column=destination_column,
            transformation_expression=transformation_expression,
        )
        self.column_mappings.append(mapping)
        self.touch()
        return mapping

    def direct_mappings(self) -> list[ColumnLineage]:
        """Return only columns with no transformation (direct copy)."""
        return [m for m in self.column_mappings if m.transformation_expression is None]

    def transformed_mappings(self) -> list[ColumnLineage]:
        """Return only columns with an explicit transformation expression."""
        return [m for m in self.column_mappings if m.transformation_expression is not None]
```

```python
# platform/domain/lineage/lineage_repository.py
from __future__ import annotations
from typing import Protocol, runtime_checkable
from platform.domain.lineage.lineage_mapping import LineageMapping


@runtime_checkable
class LineageRepository(Protocol):
    """Repository interface for LineageMapping persistence."""
    async def save(self, mapping: LineageMapping) -> LineageMapping: ...
    async def find_by_pipeline_id(self, pipeline_id: str) -> list[LineageMapping]: ...
    async def find_by_destination_object_id(self, destination_object_id: str) -> list[LineageMapping]: ...
```

- [ ] **Step 2: Escrever testes unitários**

```python
# tests/unit/domain/lineage/test_lineage_mapping.py
from __future__ import annotations
import uuid
import pytest
from platform.domain.lineage.lineage_mapping import ColumnLineage, LineageMapping


def _mapping() -> LineageMapping:
    return LineageMapping(
        id=str(uuid.uuid4()),
        pipeline_id="pipe-1",
        source_object_id="src-obj-1",
        destination_object_id="dst-obj-1",
    )


def test_add_direct_mapping() -> None:
    m = _mapping()
    col = m.add_mapping(source_column="email", destination_column="email")
    assert col.transformation_expression is None
    assert len(m.direct_mappings()) == 1


def test_add_transformed_mapping() -> None:
    m = _mapping()
    m.add_mapping("cpf", "document_hash", transformation_expression="SHA256(cpf)")
    assert len(m.transformed_mappings()) == 1
    assert m.transformed_mappings()[0].source_column == "cpf"


def test_lineage_mapping_separates_direct_from_transformed() -> None:
    m = _mapping()
    m.add_mapping("name", "name")                          # direct
    m.add_mapping("cpf", "doc_hash", "SHA256(cpf)")        # transformed
    m.add_mapping("birth_date", "age", "DATEDIFF(NOW(), birth_date) / 365")  # transformed
    assert len(m.direct_mappings()) == 1
    assert len(m.transformed_mappings()) == 2
```

- [ ] **Step 3: Rodar testes, commit**

```bash
uv run pytest tests/unit/domain/lineage/ -v
git add platform/domain/lineage/ tests/unit/domain/lineage/
git commit -m "feat: add LineageMapping domain with ColumnLineage for explicit column-level lineage"
```

---

## Task 4: Infrastructure — Models, Repositories, UnitOfWork atualizado

**Mudanças em relação à v1:**
- `DataObjectModel` sem `pipeline_id`, sem `role`
- `PipelineObjectModel` (tabela associativa N:M)
- `LineageMappingModel` novo
- `UnitOfWork` com `lineage` repo

---

- [ ] **Step 1: Criar models ORM**

```python
# platform/infrastructure/persistence/models/data_object_model.py
from __future__ import annotations
import uuid
from datetime import datetime
from sqlalchemy import DateTime, ForeignKey, JSON, String, Boolean
from sqlalchemy.orm import Mapped, mapped_column
from platform.infrastructure.persistence.base_model import Base, TimestampMixin

class DataObjectModel(Base, TimestampMixin):
    """ORM model for DataObject. No pipeline_id, no role — relationship managed by PipelineObjectModel."""
    __tablename__ = "data_objects"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    asset_id: Mapped[str] = mapped_column(String(36), ForeignKey("data_assets.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    type: Mapped[str] = mapped_column(String(50), nullable=False)
    description: Mapped[str] = mapped_column(String(2000), nullable=False, default="")
    policy_tags: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    last_run: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_success: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    freshness_status: Mapped[str] = mapped_column(String(20), nullable=False, default="unknown")
    auto_generated_description: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
```

```python
# platform/infrastructure/persistence/models/pipeline_object_model.py
from __future__ import annotations
from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column
from platform.infrastructure.persistence.base_model import Base, TimestampMixin

class PipelineObjectModel(Base, TimestampMixin):
    """
    Association table: Pipeline ↔ DataObject (N:M).

    role_in_pipeline: contextual role — 'source' or 'destination' — is pipeline-specific,
    not an intrinsic property of the DataObject.
    """
    __tablename__ = "pipeline_objects"
    pipeline_id: Mapped[str] = mapped_column(String(36), ForeignKey("pipelines.id"), primary_key=True)
    object_id: Mapped[str] = mapped_column(String(36), ForeignKey("data_objects.id"), primary_key=True)
    role_in_pipeline: Mapped[str] = mapped_column(String(20), nullable=False)  # "source" | "destination"
```

```python
# platform/infrastructure/persistence/models/lineage_mapping_model.py
from __future__ import annotations
import uuid
from sqlalchemy import ForeignKey, JSON, String
from sqlalchemy.orm import Mapped, mapped_column
from platform.infrastructure.persistence.base_model import Base, TimestampMixin

class LineageMappingModel(Base, TimestampMixin):
    """ORM for LineageMapping. column_mappings stored as JSON array."""
    __tablename__ = "lineage_mappings"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    pipeline_id: Mapped[str] = mapped_column(String(36), ForeignKey("pipelines.id"), nullable=False)
    source_object_id: Mapped[str] = mapped_column(String(36), ForeignKey("data_objects.id"), nullable=False)
    destination_object_id: Mapped[str] = mapped_column(String(36), ForeignKey("data_objects.id"), nullable=False)
    column_mappings: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
```

- [ ] **Step 2: Atualizar UnitOfWork Protocol**

```python
# platform/application/unit_of_work.py
from __future__ import annotations
from typing import Protocol, runtime_checkable
from platform.domain.assets.asset_repository import AssetRepository
from platform.domain.endpoints.endpoint_repository import EndpointRepository
from platform.domain.lineage.lineage_repository import LineageRepository
from platform.domain.objects.object_repository import DataObjectRepository
from platform.domain.pipelines.pipeline_repository import PipelineRepository


@runtime_checkable
class UnitOfWork(Protocol):
    assets: AssetRepository
    endpoints: EndpointRepository
    objects: DataObjectRepository
    pipelines: PipelineRepository
    lineage: LineageRepository  # [NEW]

    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...
    async def __aenter__(self) -> UnitOfWork: ...
    async def __aexit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object) -> None: ...
```

- [ ] **Step 3: Gerar migration**

```bash
make migrate-create name="add_objects_elements_pipelines_lineage"
make migrate
```

- [ ] **Step 4: Commit**

```bash
git add platform/infrastructure/persistence/ platform/application/unit_of_work.py
git commit -m "feat: add PipelineObjectModel (N:M), LineageMappingModel, update UnitOfWork with lineage repo"
```

---

## Task 5: YAML Generator (atualizado para ExtractionConfig + SensorConfig)

```python
# platform/infrastructure/yaml_generator/pipeline_yaml_generator.py
from __future__ import annotations

import yaml
from platform.domain.pipelines.pipeline import Pipeline


class PipelineYamlGenerator:
    """
    Generates YAML configuration string from a Pipeline domain entity.
    Single source of truth for YAML output — no hand-editing of YAML.
    """

    def generate(self, pipeline: Pipeline) -> str:
        return yaml.dump(self._build_dict(pipeline), default_flow_style=False, allow_unicode=True, sort_keys=False)

    def _build_dict(self, p: Pipeline) -> dict:
        return {
            "schema_version": p.schema_version,
            "pipeline": {
                "id": p.id,
                "name": p.name,
                "type": p.type.value,
                "owner": p.owner.value,
                "schedule": self._schedule_dict(p),
                "source": self._source_dict(p),
                "destination": self._destination_dict(p),
                "transform": self._transform_dict(p),
                "compute": self._compute_dict(p),
                "quality": self._quality_dict(p),
                "airflow": self._airflow_dict(p),
                "discovery_task": {
                    "enabled": p.discovery_task.enabled,
                    "on_critical_change": p.discovery_task.on_critical_change.value,
                },
            },
        }

    def _schedule_dict(self, p: Pipeline) -> dict:
        s = p.schedule
        d: dict = {"mode": s.mode.value}
        if s.cron_schedule:
            d["cron"] = s.cron_schedule.expression
        if s.depends_on:
            d["depends_on"] = [
                {
                    "pipeline_id": dep.pipeline_id,
                    "require_same_day": dep.require_same_day,
                    "dependency_type": dep.dependency_type.value,
                }
                for dep in s.depends_on
            ]
        return d

    def _source_dict(self, p: Pipeline) -> dict:
        objects = []
        for ext in p.source_objects:
            obj: dict = {
                "object_id": ext.object_id,
                "load_strategy": ext.load_strategy.value,
                "page_size": ext.page_size,
                "compression": ext.compression,
                "encoding": ext.encoding,
            }
            if ext.watermark_column:
                obj["watermark_column"] = ext.watermark_column
            if ext.partition_column:
                obj["partition_column"] = ext.partition_column
            if ext.extraction_query:
                obj["extraction_query"] = ext.extraction_query
            if ext.sensor:
                obj["sensor"] = {
                    "query": ext.sensor.query,
                    "timeout_minutes": ext.sensor.timeout_minutes,
                    "poke_interval_seconds": ext.sensor.poke_interval_seconds,
                }
            objects.append(obj)
        return {"asset_id": p.source_asset_id, "objects": objects}

    def _destination_dict(self, p: Pipeline) -> dict:
        return {
            "asset_id": p.destination_asset_id,
            "objects": [{"object_id": d.object_id, "create_if_not_exists": d.create_if_not_exists} for d in p.destination_objects],
        }

    def _transform_dict(self, p: Pipeline) -> dict:
        t = p.transform
        d: dict = {"engine": t.engine.value}
        if t.ref:
            d["ref"] = t.ref
        return d

    def _compute_dict(self, p: Pipeline) -> dict:
        c = p.compute
        return {
            "engine": c.engine.value,
            "staging_bucket": c.staging_bucket,
            "config": {"num_workers": c.num_workers, "machine_type": c.machine_type},
        }

    def _quality_dict(self, p: Pipeline) -> dict:
        metrics = []
        for r in p.quality_rules:
            rule: dict = {"type": r.type.value}
            if r.column:
                rule["column"] = r.column
            if r.value is not None:
                rule["value"] = r.value
            metrics.append(rule)
        return {"metrics": metrics}

    def _airflow_dict(self, p: Pipeline) -> dict:
        a = p.airflow
        return {
            "retries": a.retries,
            "retry_delay_minutes": a.retry_delay_minutes,
            "execution_timeout_minutes": a.execution_timeout_minutes,
            "sla_minutes": a.sla_minutes,
            "tags": list(a.tags),
            "pool": a.pool,
        }
```

---

## Task 6: Compute Job Callbacks — Polimorfismo e Shared Module (Airflow 3)

**Design:** Funções Python compartilhadas nos templates via `shared_callbacks.py`. `ComputeJobAdapter(Protocol)` abstrai submit/monitor para diferentes compute engines. Templates usam `@task` e `@task.sensor` do Airflow 3.

---

- [ ] **Step 1: Criar ComputeJobAdapter Protocol**

```python
# platform/infrastructure/airflow_callbacks/compute_job_adapter.py
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable


class JobStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


@dataclass
class ComputeJobResult:
    """Result returned by monitor_compute_job."""
    job_id: str
    status: JobStatus
    metrics_path: str | None = None   # GCS/S3 path to metrics.json
    schema_path: str | None = None    # GCS/S3 path to schema.json
    output_path: str | None = None    # GCS/S3 path to parquet output
    error_message: str | None = None


@runtime_checkable
class ComputeJobAdapter(Protocol):
    """
    Protocol for compute job lifecycle management.

    Each compute engine (Spark, Dataflow, default) implements this.
    Ensures the same @task flow (submit → monitor → validate → read_metrics)
    works across all pipeline types and compute backends.

    Example:
        adapter = SparkComputeJobAdapter(cluster_id="cluster-1")
        job_id = adapter.submit_job(pipeline_id, extraction_configs, staging_bucket)
        result = adapter.poll_job_status(job_id)
    """

    def submit_job(
        self,
        pipeline_id: str,
        pipeline_type: str,
        config: dict[str, Any],
    ) -> str:
        """Submit the compute job and return the job_id immediately (async submit)."""
        ...

    def poll_job_status(self, job_id: str) -> ComputeJobResult:
        """
        Poll the job status once. Called by @task.sensor in monitor_compute_job.
        Returns ComputeJobResult with current status. Sensor advances when status is terminal.
        """
        ...

    def cancel_job(self, job_id: str) -> None:
        """Cancel a running job. Called by on_failure_callback."""
        ...
```

- [ ] **Step 2: Criar shared_callbacks.py — funções comuns aos 3 templates**

```python
# platform/infrastructure/airflow_callbacks/shared_callbacks.py
from __future__ import annotations

"""
Shared Airflow callback functions used by all three DAG templates (ingestion, etl, export).

Each function signature is compatible with Airflow 3 @task decorator.
All functions are pure Python — no Airflow imports inside the function body
to keep them independently testable.

XCom policy:
  - @task functions return small dicts with external storage references only.
  - Actual data lives in GCS/S3. XCom carries {"path": "gs://..."} style refs.
"""

from datetime import date, datetime, timezone
from typing import Any

from platform.infrastructure.platform_client import get_platform_client
from platform.infrastructure.quality_gate_evaluator import QualityGateEvaluator
from platform.infrastructure.monitoring_adapter import MonitoringAdapter
from platform.infrastructure.adapters.notifications.noop_notification_adapter import NoopNotificationAdapter


def check_dependencies(
    *,
    pipeline_id: str,
    depends_on: list[dict[str, Any]],
    logical_date: datetime,
) -> dict[str, Any]:
    """
    Validate upstream pipeline completions and resource availability.

    For DATASET dependencies: verify Airflow Asset event was received.
    For EXTERNAL_EVENT: verify external event payload was received.
    For MANUAL: assert manual approval flag is set.

    Returns {"dependencies_ok": True, "checked_at": "..."}.
    Raises RuntimeError if any dependency is not satisfied — fails the task.
    """
    client = get_platform_client()
    for dep in depends_on:
        if not client.pipeline_succeeded_on(
            pipeline_id=dep["pipeline_id"],
            require_same_day=dep.get("require_same_day", False),
            logical_date=logical_date,
            dependency_type=dep.get("dependency_type", "dataset"),
        ):
            raise RuntimeError(
                f"Dependency not satisfied: pipeline_id={dep['pipeline_id']!r} "
                f"require_same_day={dep.get('require_same_day')} "
                f"dependency_type={dep.get('dependency_type')}"
            )
    return {"dependencies_ok": True, "checked_at": datetime.now(tz=timezone.utc).isoformat()}


def validate_compute_execution(
    *,
    job_result: dict[str, Any],
) -> dict[str, Any]:
    """
    Validate compute job terminal state. Raises on failure/cancellation/timeout.

    Returns the job_result dict unchanged on success (for downstream XCom).
    """
    status = job_result.get("status")
    if status != "success":
        error = job_result.get("error_message", "Unknown error")
        raise RuntimeError(
            f"Compute job {job_result.get('job_id')!r} ended with status={status!r}. "
            f"Error: {error}"
        )
    return job_result


def quality_gate(
    *,
    pipeline_id: str,
    metrics: dict[str, Any],
    quality_rules: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Confront configured quality rules against metrics produced by the compute engine.

    Metrics are read from metrics.json written by the compute job.
    Raises QualityGateFailure if any rule is violated — marks task as quality_failed.
    """
    evaluator = QualityGateEvaluator()
    failures = evaluator.evaluate(metrics=metrics, rules=quality_rules)
    if failures:
        raise RuntimeError(
            f"Quality gate failed for pipeline {pipeline_id!r}. "
            f"Violations: {failures}"
        )
    return {"quality_ok": True, "violations": [], "metrics": metrics}


def emit_monitoring_and_sla(
    *,
    pipeline_id: str,
    pipeline_name: str,
    sla_minutes: int,
    metrics: dict[str, Any],
    dag_run_start: str,
) -> None:
    """
    Emit pipeline execution metrics to the monitoring platform and evaluate SLA.

    Always runs (trigger_rule=all_done) to ensure metrics are emitted even on failure.
    Sends to configured monitoring adapter (Datadog, Cloud Monitoring, etc.).
    """
    adapter = MonitoringAdapter()
    adapter.emit_pipeline_metrics(
        pipeline_id=pipeline_id,
        pipeline_name=pipeline_name,
        sla_minutes=sla_minutes,
        metrics=metrics,
        dag_run_start=dag_run_start,
    )


def success_notification(*, pipeline_id: str, pipeline_name: str, owner: str) -> None:
    """Send success notification to the pipeline owner after all tasks complete."""
    adapter = NoopNotificationAdapter()
    # Uses sync alert method — no asyncio.run()
    adapter.send_alert_sync(
        channel=owner,
        title=f"Pipeline '{pipeline_name}' completed successfully",
        message=f"pipeline_id={pipeline_id}",
        level="info",
    )


def alert_and_monitoring(context: dict[str, Any]) -> None:
    """
    Airflow on_failure_callback. Called when any task fails.

    Sends alert to the pipeline owner and emits failure metrics.
    Registered as on_failure_callback at the DAG level.
    """
    dag_run = context.get("dag_run")
    pipeline_id = context.get("params", {}).get("pipeline_id", "unknown")
    task_id = context.get("task_instance", {}).task_id if context.get("task_instance") else "unknown"
    MonitoringAdapter().emit_failure(pipeline_id=pipeline_id, failed_task_id=task_id, dag_run=dag_run)
```

- [ ] **Step 3: Criar ingestion_callbacks.py, export_callbacks.py, etl_callbacks.py**

```python
# platform/infrastructure/airflow_callbacks/ingestion_callbacks.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from platform.infrastructure.platform_client import get_platform_client
from platform.infrastructure.drift_classifier import DriftClassifier
from platform.infrastructure.airflow_callbacks.compute_job_adapter import ComputeJobAdapter
from platform.infrastructure.compute_job_factory import get_compute_adapter


def validate_source_and_discovery(
    *,
    pipeline_id: str,
    asset_id: str,
    discovery_config: dict[str, Any],
) -> dict[str, Any]:
    """
    Validate source availability and execute Discovery metadata scan.
    Returns {"available": True, "schema_snapshot": {...}, "drift_detected": bool}.
    """
    client = get_platform_client()
    result = client.run_discovery(asset_id=asset_id, pipeline_id=pipeline_id)
    return {
        "available": result["available"],
        "schema_snapshot": result["schema"],
        "drift_detected": result.get("drift_detected", False),
    }


def classify_changes_and_plan_actions(
    *,
    schema_snapshot: dict[str, Any],
    on_critical_change: str,
) -> dict[str, Any]:
    """
    Classify schema drift per spec 4.2 (informative vs critical changes).
    """
    classifier = DriftClassifier()
    result = classifier.classify(schema_snapshot=schema_snapshot, policy=on_critical_change)
    if not result["can_proceed"]:
        raise RuntimeError(f"Extraction blocked by schema drift: {result['blocked_reason']}")
    return result


def submit_compute_job(
    *,
    pipeline_id: str,
    source_objects: list[dict[str, Any]],
    compute_config: dict[str, Any],
    staging_bucket: str,
) -> dict[str, str]:
    """
    Submit the compute extraction job asynchronously.
    """
    adapter = get_compute_adapter(compute_config["engine"])
    job_id = adapter.submit_job(
        pipeline_id=pipeline_id,
        pipeline_type="ingestion",
        config={"source_objects": source_objects, "staging_bucket": staging_bucket, **compute_config},
    )
    return {"job_id": job_id, "submitted_at": datetime.now(tz=timezone.utc).isoformat()}


def load_to_data_warehouse(
    *,
    pipeline_id: str,
    destination_object_ids: list[str],
    parquet_path: str,
    schema_path: str,
) -> dict[str, Any]:
    """
    Load parquet output from compute engine into the data warehouse.
    """
    client = get_platform_client()
    return client.load_to_warehouse(
        pipeline_id=pipeline_id,
        destination_object_ids=destination_object_ids,
        parquet_path=parquet_path,
        schema_path=schema_path,
    )


def post_load_validation(
    *,
    pipeline_id: str,
    expected_rows: int,
    actual_rows: int,
    source_checksum: str | None,
    destination_checksum: str | None,
) -> dict[str, Any]:
    """
    Validate volume and checksum integrity after DW load.
    """
    if expected_rows > 0:
        delta_pct = abs(actual_rows - expected_rows) / expected_rows
        if delta_pct > 0.005:
            raise RuntimeError(
                f"post_load_validation failed: expected {expected_rows} rows, "
                f"got {actual_rows} ({delta_pct:.1%} delta exceeds 0.5% threshold)."
            )
    if source_checksum and destination_checksum and source_checksum != destination_checksum:
        raise RuntimeError("post_load_validation failed: checksum mismatch between source and destination.")
    return {"validation_ok": True, "row_delta_pct": 0.0}
```

```python
# platform/infrastructure/airflow_callbacks/etl_callbacks.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from platform.infrastructure.platform_client import get_platform_client
from platform.infrastructure.drift_classifier import DriftClassifier
from platform.infrastructure.compute_job_factory import get_transform_adapter


def validate_source_models(*, pipeline_id: str, source_asset_id: str) -> dict[str, Any]:
    """Validate that all source dbt/Dataform models exist and are fresh."""
    client = get_platform_client()
    return client.validate_source_models(pipeline_id=pipeline_id, asset_id=source_asset_id)


def classify_schema_changes(*, source_models: dict[str, Any]) -> dict[str, Any]:
    """Classify schema changes in source models before running transformation."""
    return DriftClassifier().classify_models(source_models=source_models)


def submit_transformation_job(
    *,
    pipeline_id: str,
    transform_engine: str,
    transform_ref: str,
    compute_config: dict[str, Any],
) -> dict[str, str]:
    """Submit dbt or Dataform transformation job. Returns {"job_id": ...}."""
    adapter = get_transform_adapter(transform_engine)
    job_id = adapter.submit_job(
        pipeline_id=pipeline_id,
        pipeline_type="etl",
        config={"ref": transform_ref, **compute_config},
    )
    return {"job_id": job_id, "submitted_at": datetime.now(tz=timezone.utc).isoformat()}


def publish_documentation(*, pipeline_id: str, transform_ref: str) -> None:
    """Publish updated dbt/Dataform docs to the catalog adapter."""
    client = get_platform_client()
    client.publish_transform_docs(pipeline_id=pipeline_id, transform_ref=transform_ref)
```

```python
# platform/infrastructure/airflow_callbacks/export_callbacks.py
from __future__ import annotations

from typing import Any

from platform.infrastructure.platform_client import get_platform_client


def validate_export_configuration(*, pipeline_id: str, destination_config: dict[str, Any]) -> dict[str, Any]:
    """Validate destination connectivity and format compatibility."""
    client = get_platform_client()
    return client.validate_export_config(pipeline_id=pipeline_id, destination=destination_config)


def validate_source_dataset_readiness(*, pipeline_id: str, source_object_ids: list[str]) -> dict[str, Any]:
    """Assert that source DataObjects are FRESH before starting export."""
    client = get_platform_client()
    result = client.check_freshness(object_ids=source_object_ids)
    stale = [r for r in result if r["status"] != "fresh"]
    if stale:
        raise RuntimeError(f"Export blocked: stale source objects: {[s['object_id'] for s in stale]}")
    return {"all_fresh": True}


def classify_export_actions(*, pipeline_id: str, source_snapshot: dict[str, Any]) -> dict[str, Any]:
    """Determine what to export (full vs incremental) based on destination state."""
    return {"export_mode": "incremental", "actions": []}


def publish_export_artifacts(*, pipeline_id: str, output_path: str, destination_config: dict[str, Any]) -> dict[str, Any]:
    """Deliver exported files/records to the configured destination."""
    client = get_platform_client()
    return client.deliver_export(
        pipeline_id=pipeline_id, output_path=output_path, destination=destination_config
    )


def validate_delivery(*, pipeline_id: str, delivery_result: dict[str, Any]) -> dict[str, Any]:
    """Confirm delivery completeness: record counts, file checksums, acknowledgement."""
    if not delivery_result.get("delivered", False):
        raise RuntimeError(f"Export delivery validation failed: {delivery_result.get('error')}")
    return {"delivery_ok": True}
```

- [ ] **Step 4: Rodar testes, commit**

```bash
git add platform/infrastructure/airflow_callbacks/
git commit -m "feat: add ComputeJobAdapter Protocol, shared_callbacks, ingestion/export/etl_callbacks with polimorfismo"
```

---

## Task 7: DAG Templates Airflow 3 (15-task Ingestion, 12-task ETL, 14-task Export)

**Key Airflow 3 changes:**
- `from airflow.sdk import Asset, dag, task`
- `@dag` decorator instead of `with DAG(...) as dag:`
- `@task` for Python tasks (TaskFlow API — context auto-injected)
- `@task.sensor(mode="reschedule")` for `source_readiness_sensor`
- `on_failure_callback=alert_and_monitoring` at DAG level
- `schedule=[Asset(...)]` para trigger e trigger_with_gate
- `trigger_rule="all_done"` para emit_monitoring_and_sla

---

- [ ] **Step 1: Criar _shared_macros.j2**

```jinja2
{# platform/infrastructure/dag_generator/templates/_shared_macros.j2 #}
{# Jinja2 macros reusados pelos três templates de DAG.                 #}

{%- macro dag_imports(pipeline) %}
from __future__ import annotations
from datetime import timedelta
from airflow.sdk import Asset, dag, task
{% if pipeline.schedule.mode in ("trigger", "trigger_with_gate") %}
from airflow.sdk import task as airflow_task
{% endif %}
from platform.infrastructure.airflow_callbacks.shared_callbacks import (
    check_dependencies,
    validate_compute_execution,
    quality_gate,
    emit_monitoring_and_sla,
    success_notification,
    alert_and_monitoring,
)
{%- endmacro %}

{%- macro pipeline_asset(pipeline) %}
# Airflow 3 Asset: declared as outlet on every DAG run
_PIPELINE_ASSET = Asset("platform://pipeline/{{ pipeline.id }}")
{% if pipeline.schedule.mode in ("trigger", "trigger_with_gate") %}
_UPSTREAM_ASSETS = [
{% for dep in pipeline.schedule.depends_on %}
{% if dep.dependency_type == "dataset" %}
    Asset("platform://pipeline/{{ dep.pipeline_id }}"),
{% endif %}
{% endfor %}
]
{% endif %}
{%- endmacro %}

{%- macro dag_schedule(pipeline) %}
{% if pipeline.schedule.mode == "cron" %}
    schedule="{{ pipeline.schedule.cron }}",
{% else %}
    schedule=_UPSTREAM_ASSETS,
{% endif %}
{%- endmacro %}

{%- macro dag_default_args(pipeline) %}
    default_args={
        "retries": {{ pipeline.airflow.retries }},
        "retry_delay": timedelta(minutes={{ pipeline.airflow.retry_delay_minutes }}),
        "execution_timeout": timedelta(minutes={{ pipeline.airflow.execution_timeout_minutes }}),
    },
{%- endmacro %}
```

- [ ] **Step 2: Criar ingestion_dag.py.j2 (15 tasks)**

```jinja2
{# platform/infrastructure/dag_generator/templates/ingestion_dag.py.j2 #}
{# Airflow 3 DAG — pipeline.type = "ingestion"                         #}
{# 15 tasks + on_failure_callback = alert_and_monitoring               #}
\"\"\"
Pipeline: {{ pipeline.name }}
Version: {{ pipeline.version | default('1.0') }}
Template Version: {{ template_version }}
Generated At: {{ generated_at }}
Commit Hash: {{ commit_hash }}
\"\"\"

{% from "_shared_macros.j2" import dag_imports, pipeline_asset, dag_schedule, dag_default_args %}
{{ dag_imports(pipeline) }}
from platform.infrastructure.airflow_callbacks.ingestion_callbacks import (
    validate_source_and_discovery,
    classify_changes_and_plan_actions,
    submit_compute_job,
    load_to_data_warehouse,
    post_load_validation,
)

{{ pipeline_asset(pipeline) }}

_PIPELINE_PARAMS = {
    "pipeline_id": "{{ pipeline.id }}",
    "pipeline_name": "{{ pipeline.name }}",
    "owner": "{{ pipeline.owner }}",
    "source_asset_id": "{{ pipeline.source.asset_id }}",
    "on_critical_change": "{{ pipeline.discovery_task.on_critical_change }}",
    "sla_minutes": {{ pipeline.airflow.sla_minutes }},
    "compute_config": {
        "engine": "{{ pipeline.compute.engine }}",
        "num_workers": {{ pipeline.compute.config.num_workers }},
        "machine_type": "{{ pipeline.compute.config.machine_type }}",
        "staging_bucket": "{{ pipeline.compute.staging_bucket }}",
    },
    "quality_rules": {{ pipeline.quality.metrics | tojson }},
    "source_objects": {{ pipeline.source.objects | tojson }},
    "destination_object_ids": [{% for d in pipeline.destination.objects %}"{{ d.object_id }}", {% endfor %}],
    "depends_on": {{ pipeline.schedule.depends_on | default([]) | tojson }},
}


@dag(
    dag_id="{{ pipeline.name }}",
    description="Ingestion pipeline — generated by platform. Do not edit manually.",
    {{ dag_schedule(pipeline) }}
    {{ dag_default_args(pipeline) }}
    tags={{ pipeline.airflow.tags | tojson }},
    catchup=False,
    max_active_runs=1,
    on_failure_callback=alert_and_monitoring,
    outlets=[_PIPELINE_ASSET],
    params=_PIPELINE_PARAMS,
)
def {{ pipeline.name | replace("-", "_") }}_dag():

    # ── 1. check_dependencies ────────────────────────────────────────────────
    @task(task_id="check_dependencies", pool="{{ pipeline.airflow.pool }}")
    def _check_dependencies(**context):
        return check_dependencies(
            pipeline_id="{{ pipeline.id }}",
            depends_on=context["params"]["depends_on"],
            logical_date=context["logical_date"],
        )

    # ── 2. validate_source_and_discovery ────────────────────────────────────
    @task(task_id="validate_source_and_discovery", pool="{{ pipeline.airflow.pool }}")
    def _validate_source_and_discovery(**context):
        return validate_source_and_discovery(
            pipeline_id="{{ pipeline.id }}",
            asset_id="{{ pipeline.source.asset_id }}",
            discovery_config={
                "enabled": {{ pipeline.discovery_task.enabled | tojson }},
                "on_critical_change": "{{ pipeline.discovery_task.on_critical_change }}",
            },
        )

    # ── 3. classify_changes_and_plan_actions ─────────────────────────────────
    @task(task_id="classify_changes_and_plan_actions", pool="{{ pipeline.airflow.pool }}")
    def _classify_changes(discovery_result, **context):
        return classify_changes_and_plan_actions(
            schema_snapshot=discovery_result["schema_snapshot"],
            on_critical_change="{{ pipeline.discovery_task.on_critical_change }}",
        )

{% for obj in pipeline.source.objects %}
{% if obj.sensor is defined and obj.sensor %}
    # ── 4. source_readiness_sensor (object: {{ obj.object_id }}) ─────────────
    @task.sensor(
        task_id="source_readiness_sensor_{{ obj.object_id | replace('-', '_') }}",
        mode="reschedule",
        timeout={{ obj.sensor.timeout_minutes }} * 60,
        poke_interval={{ obj.sensor.poke_interval_seconds }},
        pool="{{ pipeline.airflow.pool }}",
    )
    def _source_readiness_sensor_{{ loop.index }}(**context) -> bool:
        """
        SqlSensor via @task.sensor. Executes sensor.query on source.
        mode=reschedule: frees worker slot between pokes.
        Returns True when query result is truthy — advances the DAG.
        """
        from platform.infrastructure.platform_api_client import PlatformApiClient
        result = PlatformApiClient().execute_sensor_query(
            asset_id="{{ pipeline.source.asset_id }}",
            query="""{{ obj.sensor.query | indent(12) }}""",
        )
        return bool(result)
{% endif %}
{% endfor %}

    # ── 5. submit_compute_job ────────────────────────────────────────────────
    @task(task_id="submit_compute_job", pool="{{ pipeline.airflow.pool }}")
    def _submit_compute_job(classification_result, **context):
        """
        Submits compute job asynchronously. Returns {"job_id": ...}.
        Compute engine runs: extract → canonicalize → cast_types → add_timestamp
        → basic_quality → write_parquet → write_schema.json → write_metrics.json
        """
        return submit_compute_job(
            pipeline_id="{{ pipeline.id }}",
            source_objects=context["params"]["source_objects"],
            compute_config=context["params"]["compute_config"],
            staging_bucket="{{ pipeline.compute.staging_bucket }}",
        )

    # ── 6. monitor_compute_job ───────────────────────────────────────────────
    @task.sensor(
        task_id="monitor_compute_job",
        mode="reschedule",
        timeout={{ pipeline.airflow.execution_timeout_minutes }} * 60,
        poke_interval=30,
        pool="{{ pipeline.airflow.pool }}",
    )
    def _monitor_compute_job(submit_result, **context) -> bool:
        """
        Polls compute job status. mode=reschedule frees worker between polls.
        Advances when job reaches a terminal state (success, failed, timeout, cancelled).
        Terminal result is passed downstream via XCom.
        """
        from platform.infrastructure.compute_job_factory import get_compute_adapter
        adapter = get_compute_adapter("{{ pipeline.compute.engine }}")
        result = adapter.poll_job_status(submit_result["job_id"])
        context["ti"].xcom_push(key="job_result", value=result.__dict__)
        return result.status not in ("pending", "running")

    # ── 7. validate_compute_execution ────────────────────────────────────────
    @task(task_id="validate_compute_execution", pool="{{ pipeline.airflow.pool }}")
    def _validate_compute_execution(**context):
        """Checks terminal status. Raises on failure/cancellation/timeout."""
        job_result = context["ti"].xcom_pull(task_ids="monitor_compute_job", key="job_result")
        return validate_compute_execution(job_result=job_result)

    # ── 8. read_compute_metrics ──────────────────────────────────────────────
    @task(task_id="read_compute_metrics", pool="{{ pipeline.airflow.pool }}")
    def _read_compute_metrics(execution_result, **context):
        """Read metrics.json written by compute engine. Returns parsed metrics dict."""
        from platform.infrastructure.storage_reader import StorageReader
        metrics_path = execution_result.get("metrics_path")
        return StorageReader().read_json(metrics_path)

    # ── 9. quality_gate ──────────────────────────────────────────────────────
    @task(task_id="quality_gate", pool="{{ pipeline.airflow.pool }}")
    def _quality_gate(metrics, **context):
        """Confront quality_rules with compute metrics. Raises on violation."""
        return quality_gate(
            pipeline_id="{{ pipeline.id }}",
            metrics=metrics,
            quality_rules=context["params"]["quality_rules"],
        )

    # ── 10. emit_raw_lineage ─────────────────────────────────────────────────
    @task(task_id="emit_raw_lineage", pool="{{ pipeline.airflow.pool }}")
    def _emit_raw_lineage(execution_result, **context):
        """Emit column-level lineage from schema.json (direct mapping, no transformations)."""
        from platform.infrastructure.platform_api_client import PlatformApiClient
        PlatformApiClient().emit_raw_lineage(
            pipeline_id="{{ pipeline.id }}",
            source_object_ids=[{% for obj in pipeline.source.objects %}"{{ obj.object_id }}", {% endfor %}],
            destination_object_ids=context["params"]["destination_object_ids"],
            schema_path=execution_result.get("schema_path"),
        )

    # ── 11. load_to_data_warehouse ───────────────────────────────────────────
    @task(task_id="load_to_data_warehouse", pool="{{ pipeline.airflow.pool }}")
    def _load_to_data_warehouse(execution_result, **context):
        """Load parquet from compute engine output into the data warehouse."""
        return load_to_data_warehouse(
            pipeline_id="{{ pipeline.id }}",
            destination_object_ids=context["params"]["destination_object_ids"],
            parquet_path=execution_result.get("output_path"),
            schema_path=execution_result.get("schema_path"),
        )

    # ── 12. post_load_validation ─────────────────────────────────────────────
    @task(task_id="post_load_validation", pool="{{ pipeline.airflow.pool }}")
    def _post_load_validation(load_result, metrics, **context):
        """Validate row count and checksum between source metrics and DW load."""
        return post_load_validation(
            pipeline_id="{{ pipeline.id }}",
            expected_rows=metrics.get("rows_written", 0),
            actual_rows=load_result.get("rows_loaded", 0),
            source_checksum=metrics.get("checksum"),
            destination_checksum=load_result.get("checksum"),
        )

    # ── 13. emit_final_lineage ───────────────────────────────────────────────
    @task(task_id="emit_final_lineage", pool="{{ pipeline.airflow.pool }}")
    def _emit_final_lineage(**context):
        """Update lineage with final row counts and freshness_status."""
        from platform.infrastructure.platform_api_client import PlatformApiClient
        PlatformApiClient().update_freshness_status(
            pipeline_id="{{ pipeline.id }}",
            destination_object_ids=context["params"]["destination_object_ids"],
        )

    # ── 14. emit_monitoring_and_sla ──────────────────────────────────────────
    @task(task_id="emit_monitoring_and_sla", trigger_rule="all_done", pool="{{ pipeline.airflow.pool }}")
    def _emit_monitoring(metrics=None, **context):
        """Always runs — emits metrics and evaluates SLA breach even on failure."""
        emit_monitoring_and_sla(
            pipeline_id="{{ pipeline.id }}",
            pipeline_name="{{ pipeline.name }}",
            sla_minutes={{ pipeline.airflow.sla_minutes }},
            metrics=metrics or {},
            dag_run_start=str(context["dag_run"].start_date),
        )

    # ── 15. success_notification ─────────────────────────────────────────────
    @task(task_id="success_notification", pool="{{ pipeline.airflow.pool }}")
    def _success_notification(**context):
        success_notification(
            pipeline_id="{{ pipeline.id }}",
            pipeline_name="{{ pipeline.name }}",
            owner="{{ pipeline.owner }}",
        )

    # ── Task wiring ──────────────────────────────────────────────────────────
    deps = _check_dependencies()
    discovery = _validate_source_and_discovery()
    classification = _classify_changes(discovery)

{% for obj in pipeline.source.objects %}
{% if obj.sensor is defined and obj.sensor %}
    sensor_{{ loop.index }} = _source_readiness_sensor_{{ loop.index }}()
    [deps, classification] >> sensor_{{ loop.index }}
{% endif %}
{% endfor %}

    submit = _submit_compute_job(classification)
{% for obj in pipeline.source.objects %}
{% if obj.sensor is defined and obj.sensor %}
    sensor_{{ loop.index }} >> submit
{% endif %}
{% endfor %}
    [deps, classification] >> submit

    monitor = _monitor_compute_job(submit)
    validate_exec = _validate_compute_execution()
    monitor >> validate_exec

    metrics_result = _read_compute_metrics(validate_exec)
    qg = _quality_gate(metrics_result)
    raw_lineage = _emit_raw_lineage(validate_exec)

    qg >> raw_lineage

    load = _load_to_data_warehouse(validate_exec)
    qg >> load

    post_val = _post_load_validation(load, metrics_result)
    final_lineage = _emit_final_lineage()
    post_val >> final_lineage

    monitoring = _emit_monitoring(metrics_result)
    final_lineage >> monitoring

    notification = _success_notification()
    monitoring >> notification


{{ pipeline.name | replace("-", "_") }}_dag()
```

- [ ] **Step 3: Criar etl_dag.py.j2 (12 tasks)**

```jinja2
{# platform/infrastructure/dag_generator/templates/etl_dag.py.j2 #}
{# Airflow 3 DAG — pipeline.type = "etl"                          #}
{# 12 tasks + on_failure_callback = alert_and_monitoring          #}
{% from "_shared_macros.j2" import dag_imports, pipeline_asset, dag_schedule, dag_default_args %}
{{ dag_imports(pipeline) }}
from platform.infrastructure.airflow_callbacks.etl_callbacks import (
    validate_source_models,
    classify_schema_changes,
    submit_transformation_job,
    publish_documentation,
)

{{ pipeline_asset(pipeline) }}

@dag(
    dag_id="{{ pipeline.name }}",
    description="ETL pipeline — generated by platform. Do not edit manually.",
    {{ dag_schedule(pipeline) }}
    {{ dag_default_args(pipeline) }}
    tags={{ pipeline.airflow.tags | tojson }},
    catchup=False,
    max_active_runs=1,
    on_failure_callback=alert_and_monitoring,
    outlets=[_PIPELINE_ASSET],
)
def {{ pipeline.name | replace("-", "_") }}_dag():

    # ── 1. check_dependencies ───────────────────────────────────────────────
    @task(task_id="check_dependencies", pool="{{ pipeline.airflow.pool }}")
    def _check_dependencies(**context):
        return check_dependencies(
            pipeline_id="{{ pipeline.id }}",
            depends_on={{ pipeline.schedule.depends_on | default([]) | tojson }},
            logical_date=context["logical_date"],
        )

    # ── 2. validate_source_models ───────────────────────────────────────────
    @task(task_id="validate_source_models", pool="{{ pipeline.airflow.pool }}")
    def _validate_source_models(**context):
        return validate_source_models(
            pipeline_id="{{ pipeline.id }}",
            source_asset_id="{{ pipeline.source.asset_id }}",
        )

    # ── 3. classify_schema_changes ──────────────────────────────────────────
    @task(task_id="classify_schema_changes", pool="{{ pipeline.airflow.pool }}")
    def _classify_schema_changes(source_models, **context):
        return classify_schema_changes(source_models=source_models)

    # ── 4. submit_transformation_job ────────────────────────────────────────
    @task(task_id="submit_transformation_job", pool="{{ pipeline.airflow.pool }}")
    def _submit_transformation_job(classification, **context):
        return submit_transformation_job(
            pipeline_id="{{ pipeline.id }}",
            transform_engine="{{ pipeline.transform.engine }}",
            transform_ref="{{ pipeline.transform.ref }}",
            compute_config={
                "engine": "{{ pipeline.compute.engine }}",
                "num_workers": {{ pipeline.compute.config.num_workers }},
            },
        )

    # ── 5. monitor_transformation_job ──────────────────────────────────────
    @task.sensor(
        task_id="monitor_transformation_job",
        mode="reschedule",
        timeout={{ pipeline.airflow.execution_timeout_minutes }} * 60,
        poke_interval=30,
        pool="{{ pipeline.airflow.pool }}",
    )
    def _monitor_transformation_job(submit_result, **context) -> bool:
        from platform.infrastructure.compute_job_factory import get_transform_adapter
        adapter = get_transform_adapter("{{ pipeline.transform.engine }}")
        result = adapter.poll_job_status(submit_result["job_id"])
        context["ti"].xcom_push(key="job_result", value=result.__dict__)
        return result.status not in ("pending", "running")

    # ── 6. validate_transformation_execution ────────────────────────────────
    @task(task_id="validate_transformation_execution", pool="{{ pipeline.airflow.pool }}")
    def _validate_transformation_execution(**context):
        job_result = context["ti"].xcom_pull(task_ids="monitor_transformation_job", key="job_result")
        return validate_compute_execution(job_result=job_result)

    # ── 7. read_transformation_metrics ──────────────────────────────────────
    @task(task_id="read_transformation_metrics", pool="{{ pipeline.airflow.pool }}")
    def _read_transformation_metrics(execution_result, **context):
        from platform.infrastructure.storage_reader import StorageReader
        return StorageReader().read_json(execution_result.get("metrics_path"))

    # ── 8. quality_gate ─────────────────────────────────────────────────────
    @task(task_id="quality_gate", pool="{{ pipeline.airflow.pool }}")
    def _quality_gate(metrics, **context):
        return quality_gate(
            pipeline_id="{{ pipeline.id }}",
            metrics=metrics,
            quality_rules={{ pipeline.quality.metrics | tojson }},
        )

    # ── 9. publish_documentation ─────────────────────────────────────────────
    @task(task_id="publish_documentation", pool="{{ pipeline.airflow.pool }}")
    def _publish_documentation(**context):
        publish_documentation(
            pipeline_id="{{ pipeline.id }}",
            transform_ref="{{ pipeline.transform.ref }}",
        )

    # ── 10. emit_lineage ─────────────────────────────────────────────────────
    @task(task_id="emit_lineage", pool="{{ pipeline.airflow.pool }}")
    def _emit_lineage(execution_result, **context):
        from platform.infrastructure.platform_api_client import PlatformApiClient
        PlatformApiClient().emit_etl_lineage(
            pipeline_id="{{ pipeline.id }}",
            transform_ref="{{ pipeline.transform.ref }}",
            schema_path=execution_result.get("schema_path"),
        )

    # ── 11. emit_monitoring_and_sla ──────────────────────────────────────────
    @task(task_id="emit_monitoring_and_sla", trigger_rule="all_done", pool="{{ pipeline.airflow.pool }}")
    def _emit_monitoring(metrics=None, **context):
        emit_monitoring_and_sla(
            pipeline_id="{{ pipeline.id }}",
            pipeline_name="{{ pipeline.name }}",
            sla_minutes={{ pipeline.airflow.sla_minutes }},
            metrics=metrics or {},
            dag_run_start=str(context["dag_run"].start_date),
        )

    # ── 12. success_notification ─────────────────────────────────────────────
    @task(task_id="success_notification", pool="{{ pipeline.airflow.pool }}")
    def _success_notification(**context):
        success_notification(
            pipeline_id="{{ pipeline.id }}",
            pipeline_name="{{ pipeline.name }}",
            owner="{{ pipeline.owner }}",
        )

    # ── Task wiring ──────────────────────────────────────────────────────────
    deps = _check_dependencies()
    source_models = _validate_source_models()
    deps >> source_models
    classification = _classify_schema_changes(source_models)
    submit = _submit_transformation_job(classification)
    monitor = _monitor_transformation_job(submit)
    validate_exec = _validate_transformation_execution()
    monitor >> validate_exec
    metrics_result = _read_transformation_metrics(validate_exec)
    qg = _quality_gate(metrics_result)
    docs = _publish_documentation()
    qg >> docs
    lineage = _emit_lineage(validate_exec)
    docs >> lineage
    monitoring = _emit_monitoring(metrics_result)
    lineage >> monitoring
    notification = _success_notification()
    monitoring >> notification


{{ pipeline.name | replace("-", "_") }}_dag()
```

- [ ] **Step 4: Criar export_dag.py.j2 (14 tasks)**

```jinja2
{# platform/infrastructure/dag_generator/templates/export_dag.py.j2 #}
{# Airflow 3 DAG — pipeline.type = "export"                        #}
{# 14 tasks + on_failure_callback = alert_and_monitoring           #}
{% from "_shared_macros.j2" import dag_imports, pipeline_asset, dag_schedule, dag_default_args %}
{{ dag_imports(pipeline) }}
from platform.infrastructure.airflow_callbacks.export_callbacks import (
    validate_export_configuration,
    validate_source_dataset_readiness,
    classify_export_actions,
    publish_export_artifacts,
    validate_delivery,
)

{{ pipeline_asset(pipeline) }}

@dag(
    dag_id="{{ pipeline.name }}",
    description="Export pipeline — generated by platform. Do not edit manually.",
    {{ dag_schedule(pipeline) }}
    {{ dag_default_args(pipeline) }}
    tags={{ pipeline.airflow.tags | tojson }},
    catchup=False,
    max_active_runs=1,
    on_failure_callback=alert_and_monitoring,
    outlets=[_PIPELINE_ASSET],
)
def {{ pipeline.name | replace("-", "_") }}_dag():

    @task(task_id="check_dependencies", pool="{{ pipeline.airflow.pool }}")
    def _check_dependencies(**context):
        return check_dependencies(
            pipeline_id="{{ pipeline.id }}",
            depends_on={{ pipeline.schedule.depends_on | default([]) | tojson }},
            logical_date=context["logical_date"],
        )

    @task(task_id="validate_export_configuration", pool="{{ pipeline.airflow.pool }}")
    def _validate_export_configuration(**context):
        return validate_export_configuration(
            pipeline_id="{{ pipeline.id }}",
            destination_config={{ pipeline.destination | tojson }},
        )

    @task(task_id="validate_source_dataset_readiness", pool="{{ pipeline.airflow.pool }}")
    def _validate_source_dataset_readiness(**context):
        return validate_source_dataset_readiness(
            pipeline_id="{{ pipeline.id }}",
            source_object_ids=[{% for obj in pipeline.source.objects %}"{{ obj.object_id }}", {% endfor %}],
        )

    @task(task_id="classify_export_actions", pool="{{ pipeline.airflow.pool }}")
    def _classify_export_actions(export_config, source_readiness, **context):
        return classify_export_actions(
            pipeline_id="{{ pipeline.id }}",
            source_snapshot=source_readiness,
        )

    @task(task_id="submit_compute_export_job", pool="{{ pipeline.airflow.pool }}")
    def _submit_compute_export_job(export_actions, **context):
        from platform.infrastructure.compute_job_factory import get_compute_adapter
        adapter = get_compute_adapter("{{ pipeline.compute.engine }}")
        job_id = adapter.submit_job(
            pipeline_id="{{ pipeline.id }}",
            pipeline_type="export",
            config={
                "source_objects": {{ pipeline.source.objects | tojson }},
                "staging_bucket": "{{ pipeline.compute.staging_bucket }}",
                "actions": export_actions,
            },
        )
        from datetime import datetime, timezone
        return {"job_id": job_id, "submitted_at": datetime.now(tz=timezone.utc).isoformat()}

    @task.sensor(
        task_id="monitor_compute_export_job",
        mode="reschedule",
        timeout={{ pipeline.airflow.execution_timeout_minutes }} * 60,
        poke_interval=30,
        pool="{{ pipeline.airflow.pool }}",
    )
    def _monitor_compute_export_job(submit_result, **context) -> bool:
        from platform.infrastructure.compute_job_factory import get_compute_adapter
        adapter = get_compute_adapter("{{ pipeline.compute.engine }}")
        result = adapter.poll_job_status(submit_result["job_id"])
        context["ti"].xcom_push(key="job_result", value=result.__dict__)
        return result.status not in ("pending", "running")

    @task(task_id="validate_compute_execution", pool="{{ pipeline.airflow.pool }}")
    def _validate_compute_execution(**context):
        job_result = context["ti"].xcom_pull(task_ids="monitor_compute_export_job", key="job_result")
        return validate_compute_execution(job_result=job_result)

    @task(task_id="read_export_metrics", pool="{{ pipeline.airflow.pool }}")
    def _read_export_metrics(execution_result, **context):
        from platform.infrastructure.storage_reader import StorageReader
        return StorageReader().read_json(execution_result.get("metrics_path"))

    @task(task_id="quality_gate", pool="{{ pipeline.airflow.pool }}")
    def _quality_gate(metrics, **context):
        return quality_gate(
            pipeline_id="{{ pipeline.id }}",
            metrics=metrics,
            quality_rules={{ pipeline.quality.metrics | tojson }},
        )

    @task(task_id="publish_export_artifacts", pool="{{ pipeline.airflow.pool }}")
    def _publish_export_artifacts(execution_result, export_actions, **context):
        return publish_export_artifacts(
            pipeline_id="{{ pipeline.id }}",
            output_path=execution_result.get("output_path"),
            destination_config={{ pipeline.destination | tojson }},
        )

    @task(task_id="validate_delivery", pool="{{ pipeline.airflow.pool }}")
    def _validate_delivery(delivery_result, **context):
        return validate_delivery(
            pipeline_id="{{ pipeline.id }}",
            delivery_result=delivery_result,
        )

    @task(task_id="emit_export_lineage", pool="{{ pipeline.airflow.pool }}")
    def _emit_export_lineage(execution_result, **context):
        from platform.infrastructure.platform_api_client import PlatformApiClient
        PlatformApiClient().emit_export_lineage(
            pipeline_id="{{ pipeline.id }}",
            source_object_ids=[{% for obj in pipeline.source.objects %}"{{ obj.object_id }}", {% endfor %}],
            destination_object_ids=[{% for obj in pipeline.destination.objects %}"{{ obj.object_id }}", {% endfor %}],
            schema_path=execution_result.get("schema_path"),
        )

    @task(task_id="emit_monitoring_and_sla", trigger_rule="all_done", pool="{{ pipeline.airflow.pool }}")
    def _emit_monitoring(metrics=None, **context):
        emit_monitoring_and_sla(
            pipeline_id="{{ pipeline.id }}",
            pipeline_name="{{ pipeline.name }}",
            sla_minutes={{ pipeline.airflow.sla_minutes }},
            metrics=metrics or {},
            dag_run_start=str(context["dag_run"].start_date),
        )

    @task(task_id="success_notification", pool="{{ pipeline.airflow.pool }}")
    def _success_notification(**context):
        success_notification(
            pipeline_id="{{ pipeline.id }}",
            pipeline_name="{{ pipeline.name }}",
            owner="{{ pipeline.owner }}",
        )

    # ── Task wiring ──────────────────────────────────────────────────────────
    deps = _check_dependencies()
    export_config = _validate_export_configuration()
    source_ready = _validate_source_dataset_readiness()
    [deps, export_config, source_ready] >> (export_actions := _classify_export_actions(export_config, source_ready))
    submit = _submit_compute_export_job(export_actions)
    monitor = _monitor_compute_export_job(submit)
    validate_exec = _validate_compute_execution()
    monitor >> validate_exec
    metrics_result = _read_export_metrics(validate_exec)
    qg = _quality_gate(metrics_result)
    artifacts = _publish_export_artifacts(validate_exec, export_actions)
    qg >> artifacts
    delivery_val = _validate_delivery(artifacts)
    lineage = _emit_export_lineage(validate_exec)
    delivery_val >> lineage
    monitoring = _emit_monitoring(metrics_result)
    lineage >> monitoring
    notification = _success_notification()
    monitoring >> notification


{{ pipeline.name | replace("-", "_") }}_dag()
```

- [ ] **Step 5: Escrever testes do DagGenerator**

```python
# tests/unit/infrastructure/test_dag_generator.py
from __future__ import annotations

import uuid
import pytest

from platform.domain.pipelines.extraction_config import ExtractionConfig
from platform.domain.pipelines.load_strategy import LoadStrategy
from platform.domain.pipelines.pipeline import Pipeline
from platform.domain.pipelines.pipeline_type import PipelineType
from platform.domain.pipelines.schedule_config import ScheduleConfig
from platform.domain.pipelines.schedule_mode import ScheduleMode
from platform.domain.pipelines.sensor_config import SensorConfig
from platform.domain.pipelines.transform_config import TransformConfig
from platform.domain.pipelines.transform_engine import TransformEngine
from platform.domain.shared.value_objects import CronSchedule, EmailAddress
from platform.infrastructure.dag_generator.dag_generator import DagGenerator
from platform.infrastructure.yaml_generator.pipeline_yaml_generator import PipelineYamlGenerator


def _yaml(pipeline: Pipeline) -> str:
    return PipelineYamlGenerator().generate(pipeline)


def _make_pipeline(pipeline_type: PipelineType, sensor: bool = False) -> Pipeline:
    sensor_cfg = SensorConfig(query="SELECT 1 FROM batch WHERE done=1") if sensor else None
    extraction = ExtractionConfig(
        object_id="obj-1",
        load_strategy=LoadStrategy.INCREMENTAL,
        sensor=sensor_cfg,
    )
    return Pipeline(
        id=str(uuid.uuid4()),
        name=f"test_{pipeline_type.value}",
        type=pipeline_type,
        owner=EmailAddress("ae@co.com"),
        schedule=ScheduleConfig(mode=ScheduleMode.CRON, cron_schedule=CronSchedule("0 6 * * *")),
        source_objects=[extraction] if pipeline_type != PipelineType.ETL else [],
        transform=TransformConfig(engine=TransformEngine.DBT, ref="models/test.sql") if pipeline_type == PipelineType.ETL else TransformConfig(),
    )


def test_ingestion_dag_uses_airflow3_dag_decorator() -> None:
    dag_code = DagGenerator().generate(_yaml(_make_pipeline(PipelineType.INGESTION)))
    assert "@dag(" in dag_code
    assert "from airflow.sdk import" in dag_code
    assert "Asset(" in dag_code


def test_ingestion_dag_has_all_15_tasks() -> None:
    dag_code = DagGenerator().generate(_yaml(_make_pipeline(PipelineType.INGESTION)))
    required_tasks = [
        "check_dependencies", "validate_source_and_discovery", "classify_changes_and_plan_actions",
        "submit_compute_job", "monitor_compute_job", "validate_compute_execution",
        "read_compute_metrics", "quality_gate", "emit_raw_lineage",
        "load_to_data_warehouse", "post_load_validation", "emit_final_lineage",
        "emit_monitoring_and_sla", "success_notification",
    ]
    for task_name in required_tasks:
        assert task_name in dag_code, f"Missing task: {task_name}"


def test_ingestion_dag_with_sensor_generates_task_sensor() -> None:
    dag_code = DagGenerator().generate(_yaml(_make_pipeline(PipelineType.INGESTION, sensor=True)))
    assert "@task.sensor" in dag_code
    assert "source_readiness_sensor" in dag_code
    assert 'mode="reschedule"' in dag_code


def test_etl_dag_uses_dag_decorator_and_has_12_tasks() -> None:
    dag_code = DagGenerator().generate(_yaml(_make_pipeline(PipelineType.ETL)))
    assert "@dag(" in dag_code
    required = ["check_dependencies", "validate_source_models", "classify_schema_changes",
                "submit_transformation_job", "monitor_transformation_job",
                "validate_transformation_execution", "read_transformation_metrics",
                "quality_gate", "publish_documentation", "emit_lineage",
                "emit_monitoring_and_sla", "success_notification"]
    for t in required:
        assert t in dag_code


def test_export_dag_has_on_failure_callback() -> None:
    dag_code = DagGenerator().generate(_yaml(_make_pipeline(PipelineType.EXPORT)))
    assert "on_failure_callback=alert_and_monitoring" in dag_code


def test_all_dags_have_outlets_pipeline_asset() -> None:
    for ptype in [PipelineType.INGESTION, PipelineType.ETL, PipelineType.EXPORT]:
        dag_code = DagGenerator().generate(_yaml(_make_pipeline(ptype)))
        assert "outlets=[_PIPELINE_ASSET]" in dag_code
        assert "platform://pipeline/" in dag_code
```

- [ ] **Step 6: Rodar testes**

```bash
uv run pytest tests/unit/infrastructure/test_dag_generator.py -v
```

Esperado: `6 passed`

- [ ] **Step 7: Commit**

```bash
git add platform/infrastructure/dag_generator/ platform/infrastructure/airflow_callbacks/ tests/unit/infrastructure/test_dag_generator.py
git commit -m "feat: add Airflow 3 DAG templates (ingestion 15-task, etl 12-task, export 14-task) with ComputeJobAdapter and shared_callbacks polimorfismo"
```

---

---

## Task 8: Domain — PipelineRun (Dashboard Operacional)

**Propósito:** Entidade que registra cada execução do pipeline para alimentar dashboards operacionais. Persistida pelo `emit_monitoring_and_sla` (`trigger_rule=all_done`) — garante que mesmo em falha parcial a execução seja registrada.

**Campos para dashboard:**
- `last_run_at` — sempre atualizado, inclusive em falha
- `last_success_at` — atualizado apenas em `SUCCESS`
- `status` — `running | success | failed | quality_failed | partial`
- `failed_task` — task que causou a falha (para drill-down)
- `optional_failures` — lista de optional tasks que falharam (status `partial`)
- `metrics` — métricas do compute engine (linhas, bytes, checksums)

---

- [ ] **Step 1: Criar PipelineRunStatus enum**

```python
# platform/domain/pipelines/pipeline_run_status.py
from __future__ import annotations
from enum import StrEnum


class PipelineRunStatus(StrEnum):
    """
    Operational status of a PipelineRun execution.

    RUNNING: DAG is currently executing.
    SUCCESS: all mandatory tasks succeeded. Optional tasks may have had soft_fail.
    FAILED: at least one mandatory task failed. Data result is unreliable.
    QUALITY_FAILED: quality_gate blocked the load. Data was not persisted to DW.
    PARTIAL: all mandatory tasks succeeded, but at least one optional task soft_failed.
      Data is available in DW; observability tasks (lineage, monitoring, notification) failed.
    """

    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    QUALITY_FAILED = "quality_failed"
    PARTIAL = "partial"
```

- [ ] **Step 2: Criar PipelineRun entity**

```python
# platform/domain/pipelines/pipeline_run.py
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from platform.domain.pipelines.pipeline_run_status import PipelineRunStatus
from platform.domain.shared.auditable import Auditable


@dataclass
class PipelineRun(Auditable):
    """
    Operational execution record for a Pipeline DAG run.

    Created when emit_monitoring_and_sla fires (trigger_rule=all_done).
    Always persisted — even if the pipeline failed — so the dashboard always has
    a record of the last attempt (last_run_at) vs the last success (last_success_at).

    Enables operational dashboards showing:
    - Pipeline health at a glance (status per pipeline)
    - Time since last success (freshness SLA)
    - Failure triage (failed_task for root cause drill-down)
    - Quality trends (quality_violations over time)
    - Optional task degradation (partial runs where observability failed)

    PipelineRun is written by the Airflow callback layer (emit_monitoring_and_sla)
    and read by the platform API for dashboard endpoints.
    It is NOT part of the core data processing path.
    """

    id: str
    pipeline_id: str
    pipeline_name: str
    pipeline_type: str                               # "ingestion" | "etl" | "export"
    dag_run_id: str                                  # Airflow DAG run ID for tracing
    status: PipelineRunStatus
    started_at: datetime
    finished_at: datetime | None = None
    failed_task: str | None = None                   # Task ID of the first mandatory task failure
    optional_failures: list[str] = field(default_factory=list)  # Optional tasks that soft_failed
    quality_violations: list[str] = field(default_factory=list)
    metrics: dict = field(default_factory=dict)       # rows_written, bytes_written, checksum, etc.
    sla_breached: bool = False
    sla_minutes: int = 90

    def is_partial(self) -> bool:
        """True when mandatory tasks succeeded but optional tasks soft_failed."""
        return self.status == PipelineRunStatus.PARTIAL

    def duration_seconds(self) -> float | None:
        """Elapsed seconds between started_at and finished_at."""
        if self.finished_at is None:
            return None
        return (self.finished_at - self.started_at).total_seconds()

    def mark_success(self, finished_at: datetime, metrics: dict) -> None:
        """Transition to SUCCESS (or PARTIAL if optional_failures is not empty)."""
        self.finished_at = finished_at
        self.metrics = metrics
        self.status = (
            PipelineRunStatus.PARTIAL
            if self.optional_failures
            else PipelineRunStatus.SUCCESS
        )
        self.touch()

    def mark_failed(self, finished_at: datetime, failed_task: str) -> None:
        """Transition to FAILED after a mandatory task failure."""
        self.finished_at = finished_at
        self.failed_task = failed_task
        self.status = PipelineRunStatus.FAILED
        self.touch()

    def mark_quality_failed(self, finished_at: datetime, violations: list[str]) -> None:
        """Transition to QUALITY_FAILED after quality_gate rejects the data."""
        self.finished_at = finished_at
        self.quality_violations = violations
        self.status = PipelineRunStatus.QUALITY_FAILED
        self.touch()
```

- [ ] **Step 3: Criar PipelineRunRepository Protocol**

```python
# platform/domain/pipelines/pipeline_run_repository.py
from __future__ import annotations
from datetime import datetime
from typing import Protocol, runtime_checkable

from platform.domain.pipelines.pipeline_run import PipelineRun
from platform.domain.pipelines.pipeline_run_status import PipelineRunStatus


@runtime_checkable
class PipelineRunRepository(Protocol):
    """
    Repository for PipelineRun persistence.

    last_run_at and last_success_at are maintained as columns on the
    PipelineRunModel to allow efficient dashboard queries without aggregation.
    """

    async def save(self, run: PipelineRun) -> PipelineRun: ...

    async def find_by_id(self, run_id: str) -> PipelineRun | None: ...

    async def find_latest_by_pipeline_id(self, pipeline_id: str) -> PipelineRun | None:
        """Return the most recent run (by started_at desc) for a given pipeline."""
        ...

    async def find_dashboard_summary(self) -> list[dict]:
        """
        Return one row per pipeline with operational dashboard fields:
        {pipeline_id, pipeline_name, status, last_run_at, last_success_at,
         failed_task, sla_breached, duration_seconds}

        Optimized for dashboard reads — does NOT load full metrics JSON.
        """
        ...
```

- [ ] **Step 4: Escrever testes unitários**

```python
# tests/unit/domain/pipelines/test_pipeline_run.py
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
import pytest

from platform.domain.pipelines.pipeline_run import PipelineRun
from platform.domain.pipelines.pipeline_run_status import PipelineRunStatus


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _run(**kwargs) -> PipelineRun:
    defaults = dict(
        id=str(uuid.uuid4()),
        pipeline_id="pipe-1",
        pipeline_name="test_pipeline",
        pipeline_type="ingestion",
        dag_run_id="manual__2026-06-30",
        status=PipelineRunStatus.RUNNING,
        started_at=_now(),
    )
    return PipelineRun(**{**defaults, **kwargs})


def test_mark_success_transitions_to_success() -> None:
    run = _run()
    run.mark_success(finished_at=_now(), metrics={"rows_written": 1000})
    assert run.status == PipelineRunStatus.SUCCESS
    assert run.metrics["rows_written"] == 1000
    assert run.finished_at is not None


def test_mark_success_with_optional_failures_transitions_to_partial() -> None:
    run = _run(optional_failures=["emit_raw_lineage", "success_notification"])
    run.mark_success(finished_at=_now(), metrics={})
    assert run.status == PipelineRunStatus.PARTIAL
    assert run.is_partial() is True


def test_mark_failed_records_failed_task() -> None:
    run = _run()
    run.mark_failed(finished_at=_now(), failed_task="load_to_data_warehouse")
    assert run.status == PipelineRunStatus.FAILED
    assert run.failed_task == "load_to_data_warehouse"


def test_mark_quality_failed_records_violations() -> None:
    run = _run()
    run.mark_quality_failed(
        finished_at=_now(),
        violations=["not_null violation on column 'customer_id'"],
    )
    assert run.status == PipelineRunStatus.QUALITY_FAILED
    assert len(run.quality_violations) == 1


def test_duration_seconds_none_when_not_finished() -> None:
    run = _run()
    assert run.duration_seconds() is None


def test_duration_seconds_when_finished() -> None:
    start = _now()
    end = start + timedelta(minutes=5)
    run = _run(started_at=start, finished_at=end, status=PipelineRunStatus.SUCCESS)
    assert run.duration_seconds() == pytest.approx(300.0, abs=1.0)
```

- [ ] **Step 5: Rodar testes, commit**

```bash
uv run pytest tests/unit/domain/pipelines/test_pipeline_run.py -v
git add platform/domain/pipelines/pipeline_run*.py tests/unit/domain/pipelines/test_pipeline_run.py
git commit -m "feat: add PipelineRun domain entity with status transitions (success/failed/quality_failed/partial) for operational dashboard"
```

---

## Task 9: Infrastructure — PipelineRun ORM + Repository

---

- [ ] **Step 1: Criar PipelineRunModel**

```python
# platform/infrastructure/persistence/models/pipeline_run_model.py
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from platform.infrastructure.persistence.base_model import Base, TimestampMixin


class PipelineRunModel(Base, TimestampMixin):
    """
    ORM model for PipelineRun operational records.

    last_run_at and last_success_at are denormalized columns (not computed)
    to allow fast dashboard queries without GROUP BY aggregations.

    These columns are maintained by the UpsertPipelineRunSummary operation
    inside SqlPipelineRunRepository.upsert_summary(), called after each save().

    Index strategy:
    - pipeline_id: dashboard queries filter by pipeline
    - started_at DESC: latest run lookup
    - status: filtering by healthy/degraded pipelines
    """

    __tablename__ = "pipeline_runs"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    pipeline_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("pipelines.id"), nullable=False, index=True
    )
    pipeline_name: Mapped[str] = mapped_column(String(255), nullable=False)
    pipeline_type: Mapped[str] = mapped_column(String(50), nullable=False)
    dag_run_id: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    # Denormalized for dashboard efficiency
    last_run_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    last_success_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    failed_task: Mapped[str | None] = mapped_column(String(255), nullable=True)
    optional_failures: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    quality_violations: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    metrics: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    sla_breached: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    sla_minutes: Mapped[int] = mapped_column(Integer, nullable=False, default=90)
```

- [ ] **Step 2: Criar SqlPipelineRunRepository**

```python
# platform/infrastructure/persistence/repositories/sql_pipeline_run_repository.py
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from platform.domain.pipelines.pipeline_run import PipelineRun
from platform.domain.pipelines.pipeline_run_status import PipelineRunStatus
from platform.infrastructure.persistence.models.pipeline_run_model import PipelineRunModel


def _to_domain(m: PipelineRunModel) -> PipelineRun:
    return PipelineRun(
        id=m.id,
        pipeline_id=m.pipeline_id,
        pipeline_name=m.pipeline_name,
        pipeline_type=m.pipeline_type,
        dag_run_id=m.dag_run_id,
        status=PipelineRunStatus(m.status),
        started_at=m.started_at,
        finished_at=m.finished_at,
        failed_task=m.failed_task,
        optional_failures=m.optional_failures,
        quality_violations=m.quality_violations,
        metrics=m.metrics,
        sla_breached=m.sla_breached,
        sla_minutes=m.sla_minutes,
        created_at=m.created_at,
        updated_at=m.updated_at,
    )


class SqlPipelineRunRepository:
    """
    SQLAlchemy implementation of PipelineRunRepository.

    last_run_at is always set to now().
    last_success_at is updated only for SUCCESS and PARTIAL statuses.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, run: PipelineRun) -> PipelineRun:
        now = datetime.now(tz=timezone.utc)
        model = PipelineRunModel(
            id=run.id,
            pipeline_id=run.pipeline_id,
            pipeline_name=run.pipeline_name,
            pipeline_type=run.pipeline_type,
            dag_run_id=run.dag_run_id,
            status=run.status.value,
            started_at=run.started_at,
            finished_at=run.finished_at,
            last_run_at=now,
            last_success_at=(
                now
                if run.status in (PipelineRunStatus.SUCCESS, PipelineRunStatus.PARTIAL)
                else None
            ),
            failed_task=run.failed_task,
            optional_failures=run.optional_failures,
            quality_violations=run.quality_violations,
            metrics=run.metrics,
            sla_breached=run.sla_breached,
            sla_minutes=run.sla_minutes,
        )
        self._session.add(model)
        await self._session.flush()
        return _to_domain(model)

    async def find_by_id(self, run_id: str) -> PipelineRun | None:
        result = await self._session.execute(
            select(PipelineRunModel).where(PipelineRunModel.id == run_id)
        )
        m = result.scalar_one_or_none()
        return _to_domain(m) if m else None

    async def find_latest_by_pipeline_id(self, pipeline_id: str) -> PipelineRun | None:
        result = await self._session.execute(
            select(PipelineRunModel)
            .where(PipelineRunModel.pipeline_id == pipeline_id)
            .order_by(PipelineRunModel.started_at.desc())
            .limit(1)
        )
        m = result.scalar_one_or_none()
        return _to_domain(m) if m else None

    async def find_dashboard_summary(self) -> list[dict]:
        """
        Return one lightweight row per pipeline for the operational dashboard.

        Does NOT load metrics JSON — only the fields needed for at-a-glance health.
        """
        result = await self._session.execute(
            select(
                PipelineRunModel.pipeline_id,
                PipelineRunModel.pipeline_name,
                PipelineRunModel.status,
                PipelineRunModel.last_run_at,
                PipelineRunModel.last_success_at,
                PipelineRunModel.failed_task,
                PipelineRunModel.sla_breached,
                PipelineRunModel.started_at,
                PipelineRunModel.finished_at,
            )
            .distinct(PipelineRunModel.pipeline_id)
            .order_by(
                PipelineRunModel.pipeline_id,
                PipelineRunModel.started_at.desc(),
            )
        )
        return [row._asdict() for row in result.all()]
```

- [ ] **Step 3: Gerar migration, commit**

```bash
make migrate-create name="add_pipeline_runs_table"
make migrate
git add platform/infrastructure/persistence/models/pipeline_run_model.py \
        platform/infrastructure/persistence/repositories/sql_pipeline_run_repository.py \
        migrations/
git commit -m "feat: add PipelineRunModel with last_run_at, last_success_at, failed_task for operational dashboard"
```

---

## Task 10: `get_platform_client()` Factory com `@cache`

**Propósito:** Substituir `PlatformApiClient()` direto em todos os callbacks por `get_platform_client()`. Facilita mock em testes (`lru_cache.cache_clear()`), garante singleton eficiente e não gera nova instância por chamada.

---

- [ ] **Step 1: Criar `platform_client.py`**

```python
# platform/infrastructure/platform_client.py
from __future__ import annotations

from functools import lru_cache

from platform.infrastructure.adapters.platform_api_client import PlatformApiClient


@lru_cache(maxsize=1)
def get_platform_client() -> PlatformApiClient:
    """
    Cached singleton for PlatformApiClient.

    Use lru_cache instead of @cache to allow cache_clear() in tests:
        from platform.infrastructure.platform_client import get_platform_client
        get_platform_client.cache_clear()  # reset singleton for test isolation

    All Airflow callbacks must import via this function — never instantiate
    PlatformApiClient() directly. This enforces a single mock point for tests.

    Example:
        client = get_platform_client()
        result = client.run_discovery(asset_id=..., pipeline_id=...)
    """
    return PlatformApiClient()
```

- [ ] **Step 2: Atualizar todos os callbacks para usar `get_platform_client()`**

```python
# Padrão correto em todos os callbacks:
from platform.infrastructure.platform_client import get_platform_client

client = get_platform_client()
result = client.run_discovery(...)

# NUNCA:
from platform.infrastructure.platform_api_client import PlatformApiClient
client = PlatformApiClient()  # ← proibido em callbacks
```

- [ ] **Step 3: Atualizar shared_callbacks.py com client factory e notificação síncrona**

```python
# platform/infrastructure/airflow_callbacks/shared_callbacks.py
from __future__ import annotations

"""
Shared Airflow callback functions used by all three DAG templates.

All functions use get_platform_client() — never PlatformApiClient() directly.
success_notification is synchronous — no asyncio.run().
XCom policy: @task functions return small dicts with external storage refs only.
"""

from datetime import datetime, timezone
from typing import Any


def check_dependencies(
    *,
    pipeline_id: str,
    depends_on: list[dict[str, Any]],
    logical_date: datetime,
) -> dict[str, Any]:
    """
    Validate upstream pipeline completions and resource availability.
    MANDATORY — failure blocks the DAG.
    """
    from platform.infrastructure.platform_client import get_platform_client
    client = get_platform_client()
    for dep in depends_on:
        if not client.pipeline_succeeded_on(
            pipeline_id=dep["pipeline_id"],
            require_same_day=dep.get("require_same_day", False),
            logical_date=logical_date,
            dependency_type=dep.get("dependency_type", "dataset"),
        ):
            raise RuntimeError(
                f"Dependency not satisfied: pipeline_id={dep['pipeline_id']!r} "
                f"type={dep.get('dependency_type')!r}"
            )
    return {"dependencies_ok": True, "checked_at": datetime.now(tz=timezone.utc).isoformat()}


def validate_compute_execution(*, job_result: dict[str, Any]) -> dict[str, Any]:
    """
    Validate compute job terminal state. Raises on failure/cancellation/timeout.
    MANDATORY — failure blocks the DAG.
    """
    status = job_result.get("status")
    if status != "success":
        error = job_result.get("error_message", "Unknown error")
        raise RuntimeError(
            f"Compute job {job_result.get('job_id')!r} ended with status={status!r}. "
            f"Error: {error}"
        )
    return job_result


def quality_gate(
    *,
    pipeline_id: str,
    metrics: dict[str, Any],
    quality_rules: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Confront configured quality rules against compute engine metrics.
    MANDATORY — quality_failed blocks the DAG and downstream pipelines.

    metrics may be empty if read_compute_metrics soft_failed. In that case,
    rules that require metrics (e.g., row_count_min) are skipped with a warning.
    Rules that are metric-independent (e.g., not_null via schema.json) still execute.
    """
    from platform.infrastructure.quality_gate_evaluator import QualityGateEvaluator
    evaluator = QualityGateEvaluator()
    failures = evaluator.evaluate(metrics=metrics, rules=quality_rules)
    if failures:
        raise RuntimeError(
            f"Quality gate failed for pipeline {pipeline_id!r}. Violations: {failures}"
        )
    return {"quality_ok": True, "violations": [], "metrics": metrics}


def emit_monitoring_and_sla(
    *,
    pipeline_id: str,
    pipeline_name: str,
    pipeline_type: str,
    dag_run_id: str,
    sla_minutes: int,
    metrics: dict[str, Any],
    dag_run_start: str,
    status: str,
    failed_task: str | None,
    optional_failures: list[str],
    quality_violations: list[str],
) -> None:
    """
    Persist PipelineRun record and emit metrics to monitoring platform.
    OPTIONAL (soft_fail=True) + trigger_rule=all_done.

    Always runs — even if the pipeline failed — to maintain operational dashboard.
    Persists PipelineRun with the final status determined from context.
    """
    import uuid
    from datetime import datetime, timezone

    from platform.infrastructure.platform_client import get_platform_client
    from platform.infrastructure.monitoring_adapter import MonitoringAdapter

    now = datetime.now(tz=timezone.utc)
    client = get_platform_client()

    # Persist PipelineRun for dashboard
    run_record = {
        "id": str(uuid.uuid4()),
        "pipeline_id": pipeline_id,
        "pipeline_name": pipeline_name,
        "pipeline_type": pipeline_type,
        "dag_run_id": dag_run_id,
        "status": status,
        "started_at": dag_run_start,
        "finished_at": now.isoformat(),
        "failed_task": failed_task,
        "optional_failures": optional_failures,
        "quality_violations": quality_violations,
        "metrics": metrics,
        "sla_minutes": sla_minutes,
    }
    client.upsert_pipeline_run(run_record)

    # Emit to external monitoring (synchronous)
    MonitoringAdapter().emit_pipeline_metrics(
        pipeline_id=pipeline_id,
        pipeline_name=pipeline_name,
        sla_minutes=sla_minutes,
        metrics=metrics,
        dag_run_start=dag_run_start,
        status=status,
    )


def success_notification(*, pipeline_id: str, pipeline_name: str, owner: str) -> None:
    """
    Send synchronous success notification to pipeline owner.
    OPTIONAL (soft_fail=True).

    Synchronous — no asyncio.run(). The notification adapter is called directly.
    If the notification adapter is async-only, wrap via adapter.notify_sync()
    which runs in a dedicated thread pool inside the adapter.
    """
    from platform.infrastructure.adapters.notifications.notification_adapter import get_notification_adapter
    adapter = get_notification_adapter()
    # Synchronous call — adapter must implement send_sync() or use threading internally
    adapter.send_sync(
        channel=owner,
        title=f"✅ Pipeline '{pipeline_name}' completed successfully",
        message=f"pipeline_id={pipeline_id}",
        level="info",
    )


def alert_and_monitoring(context: dict[str, Any]) -> None:
    """
    Airflow on_failure_callback. Called by Airflow when any mandatory task fails.

    Registered at the @dag level — not per-task. Sends failure alert and emits
    failure metric to monitoring. Does NOT persist PipelineRun (that's done by
    emit_monitoring_and_sla which runs with trigger_rule=all_done).
    """
    from platform.infrastructure.platform_client import get_platform_client
    from platform.infrastructure.monitoring_adapter import MonitoringAdapter

    pipeline_id = context.get("params", {}).get("pipeline_id", "unknown")
    ti = context.get("task_instance")
    task_id = ti.task_id if ti else "unknown"

    MonitoringAdapter().emit_failure(
        pipeline_id=pipeline_id,
        failed_task_id=task_id,
        dag_run=context.get("dag_run"),
    )
    get_platform_client().notify_failure(
        pipeline_id=pipeline_id,
        failed_task=task_id,
    )
```

- [ ] **Step 4: Testes da shared_callbacks**

```python
# tests/unit/infrastructure/test_shared_callbacks.py
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from platform.infrastructure.airflow_callbacks.shared_callbacks import (
    check_dependencies,
    quality_gate,
    validate_compute_execution,
)


def test_check_dependencies_raises_when_upstream_not_satisfied() -> None:
    mock_client = MagicMock()
    mock_client.pipeline_succeeded_on.return_value = False

    with patch("platform.infrastructure.platform_client.get_platform_client", return_value=mock_client):
        with pytest.raises(RuntimeError, match="Dependency not satisfied"):
            check_dependencies(
                pipeline_id="pipe-1",
                depends_on=[{"pipeline_id": "upstream-1", "dependency_type": "dataset"}],
                logical_date=datetime.now(tz=timezone.utc),
            )


def test_check_dependencies_passes_when_all_satisfied() -> None:
    mock_client = MagicMock()
    mock_client.pipeline_succeeded_on.return_value = True

    with patch("platform.infrastructure.platform_client.get_platform_client", return_value=mock_client):
        result = check_dependencies(
            pipeline_id="pipe-1",
            depends_on=[{"pipeline_id": "upstream-1", "dependency_type": "dataset"}],
            logical_date=datetime.now(tz=timezone.utc),
        )
        assert result["dependencies_ok"] is True


def test_validate_compute_execution_raises_on_failure() -> None:
    with pytest.raises(RuntimeError, match="status='failed'"):
        validate_compute_execution(job_result={"job_id": "j1", "status": "failed", "error_message": "OOM"})


def test_validate_compute_execution_passes_on_success() -> None:
    result = validate_compute_execution(job_result={"job_id": "j1", "status": "success", "output_path": "gs://bucket/data"})
    assert result["status"] == "success"


def test_quality_gate_raises_on_violations() -> None:
    mock_evaluator = MagicMock()
    mock_evaluator.evaluate.return_value = ["not_null violation on customer_id"]

    with patch("platform.infrastructure.quality_gate_evaluator.QualityGateEvaluator", return_value=mock_evaluator):
        with pytest.raises(RuntimeError, match="Quality gate failed"):
            quality_gate(
                pipeline_id="pipe-1",
                metrics={"rows_written": 0},
                quality_rules=[{"type": "not_null", "column": "customer_id"}],
            )
```

- [ ] **Step 5: Commit**

```bash
uv run pytest tests/unit/infrastructure/test_shared_callbacks.py -v
git add platform/infrastructure/platform_client.py platform/infrastructure/airflow_callbacks/shared_callbacks.py tests/unit/infrastructure/test_shared_callbacks.py
git commit -m "feat: add get_platform_client() @cache factory; update shared_callbacks with sync notification and PipelineRun persistence"
```

---

## Task 11: DAG Templates com TaskGroup e soft_fail (Ingestion)

**Mudanças nos templates em relação à v2:**
- `TaskGroup` para agrupamento visual e operacional
- MANDATORY tasks: `trigger_rule` default
- OPTIONAL tasks: `@task(soft_fail=True)` — falha registrada em `optional_failures`
- `emit_monitoring_and_sla` recebe `status`, `failed_task`, `optional_failures` para persistir `PipelineRun`
- `get_platform_client()` em todos os callbacks inline

---

- [ ] **Step 1: Atualizar ingestion_dag.py.j2**

```jinja2
{# platform/infrastructure/dag_generator/templates/ingestion_dag.py.j2 #}
{# Airflow 3 — pipeline.type = "ingestion"                              #}
{# 15 tasks organized in 6 TaskGroups                                   #}
{# MANDATORY: all_success (default). OPTIONAL: soft_fail=True           #}
{% from "_shared_macros.j2" import dag_imports, pipeline_asset, dag_schedule, dag_default_args %}
{{ dag_imports(pipeline) }}
from airflow.sdk import TaskGroup
from platform.infrastructure.airflow_callbacks.ingestion_callbacks import (
    validate_source_and_discovery,
    classify_changes_and_plan_actions,
    submit_compute_job,
    load_to_data_warehouse,
    post_load_validation,
)

{{ pipeline_asset(pipeline) }}

_PIPELINE_PARAMS = {
    "pipeline_id": "{{ pipeline.id }}",
    "pipeline_name": "{{ pipeline.name }}",
    "pipeline_type": "ingestion",
    "owner": "{{ pipeline.owner }}",
    "source_asset_id": "{{ pipeline.source.asset_id }}",
    "on_critical_change": "{{ pipeline.discovery_task.on_critical_change }}",
    "sla_minutes": {{ pipeline.airflow.sla_minutes }},
    "compute_config": {
        "engine": "{{ pipeline.compute.engine }}",
        "num_workers": {{ pipeline.compute.config.num_workers }},
        "machine_type": "{{ pipeline.compute.config.machine_type }}",
        "staging_bucket": "{{ pipeline.compute.staging_bucket }}",
    },
    "quality_rules": {{ pipeline.quality.metrics | tojson }},
    "source_objects": {{ pipeline.source.objects | tojson }},
    "destination_object_ids": [{% for d in pipeline.destination.objects %}"{{ d.object_id }}", {% endfor %}],
    "depends_on": {{ pipeline.schedule.depends_on | default([]) | tojson }},
}


@dag(
    dag_id="{{ pipeline.name }}",
    description="Ingestion pipeline — generated by platform. Do not edit manually.",
    {{ dag_schedule(pipeline) }}
    {{ dag_default_args(pipeline) }}
    tags={{ pipeline.airflow.tags | tojson }},
    catchup=False,
    max_active_runs=1,
    on_failure_callback=alert_and_monitoring,
    outlets=[_PIPELINE_ASSET],
    params=_PIPELINE_PARAMS,
)
def {{ pipeline.name | replace("-", "_") }}_dag():

    # ═══════════════════════════════════════════════════════════════════════
    # TaskGroup: pre_flight — MANDATORY
    # ═══════════════════════════════════════════════════════════════════════
    with TaskGroup(group_id="pre_flight") as pre_flight:

        @task(task_id="check_dependencies", pool="{{ pipeline.airflow.pool }}")
        def _check_dependencies(**context):
            """MANDATORY: Verifies all upstream pipeline dependencies are satisfied."""
            return check_dependencies(
                pipeline_id="{{ pipeline.id }}",
                depends_on=context["params"]["depends_on"],
                logical_date=context["logical_date"],
            )

        @task(task_id="validate_source_and_discovery", pool="{{ pipeline.airflow.pool }}")
        def _validate_source_and_discovery(**context):
            """MANDATORY: Validates source availability and runs metadata discovery."""
            return validate_source_and_discovery(
                pipeline_id="{{ pipeline.id }}",
                asset_id="{{ pipeline.source.asset_id }}",
                discovery_config={
                    "enabled": {{ pipeline.discovery_task.enabled | tojson }},
                    "on_critical_change": "{{ pipeline.discovery_task.on_critical_change }}",
                },
            )

        @task(task_id="classify_changes_and_plan_actions", pool="{{ pipeline.airflow.pool }}")
        def _classify_changes(discovery_result, **context):
            """MANDATORY: Classifies schema drift. Blocks on critical incompatible changes."""
            return classify_changes_and_plan_actions(
                schema_snapshot=discovery_result["schema_snapshot"],
                on_critical_change="{{ pipeline.discovery_task.on_critical_change }}",
            )

        _check_dependencies() >> (disc := _validate_source_and_discovery())
        _classify_changes(disc)

    # ═══════════════════════════════════════════════════════════════════════
    # TaskGroup: source_readiness — MANDATORY (generated only if sensor configured)
    # ═══════════════════════════════════════════════════════════════════════
{% set has_sensor = pipeline.source.objects | selectattr("sensor", "defined") | selectattr("sensor") | list | length > 0 %}
{% if has_sensor %}
    with TaskGroup(group_id="source_readiness") as source_readiness:
{% for obj in pipeline.source.objects %}
{% if obj.sensor is defined and obj.sensor %}
        @task.sensor(
            task_id="source_readiness_sensor_{{ obj.object_id | replace('-', '_') }}",
            mode="reschedule",
            timeout={{ obj.sensor.timeout_minutes }} * 60,
            poke_interval={{ obj.sensor.poke_interval_seconds }},
            pool="{{ pipeline.airflow.pool }}",
        )
        def _source_readiness_sensor_{{ loop.index }}(**context) -> bool:
            """MANDATORY: Polls source for readiness (batch complete, partition available)."""
            from platform.infrastructure.platform_client import get_platform_client
            return bool(get_platform_client().execute_sensor_query(
                asset_id="{{ pipeline.source.asset_id }}",
                query="""{{ obj.sensor.query | indent(16) }}""",
            ))
{% endif %}
{% endfor %}
    pre_flight >> source_readiness
{% endif %}

    # ═══════════════════════════════════════════════════════════════════════
    # TaskGroup: compute_engine — MANDATORY
    # ═══════════════════════════════════════════════════════════════════════
    with TaskGroup(group_id="compute_engine") as compute_engine_group:

        @task(task_id="submit_compute_job", pool="{{ pipeline.airflow.pool }}")
        def _submit_compute_job(**context):
            """
            MANDATORY: Submits async compute job. Returns {"job_id": ...}.
            Compute engine runs: extract → canonicalize → cast_types → add_timestamp
            → basic_quality → write_parquet → write_schema.json → write_metrics.json
            """
            return submit_compute_job(
                pipeline_id="{{ pipeline.id }}",
                source_objects=context["params"]["source_objects"],
                compute_config=context["params"]["compute_config"],
                staging_bucket="{{ pipeline.compute.staging_bucket }}",
            )

        @task.sensor(
            task_id="monitor_compute_job",
            mode="reschedule",
            timeout={{ pipeline.airflow.execution_timeout_minutes }} * 60,
            poke_interval=30,
            pool="{{ pipeline.airflow.pool }}",
        )
        def _monitor_compute_job(submit_result, **context) -> bool:
            """MANDATORY: Polls job until terminal state. Passes job_result via XCom."""
            from platform.infrastructure.compute_job_factory import get_compute_adapter
            adapter = get_compute_adapter("{{ pipeline.compute.engine }}")
            result = adapter.poll_job_status(submit_result["job_id"])
            context["ti"].xcom_push(key="job_result", value=result.__dict__)
            return result.status not in ("pending", "running")

        @task(task_id="validate_compute_execution", pool="{{ pipeline.airflow.pool }}")
        def _validate_compute_execution(**context):
            """MANDATORY: Raises if job ended in failure, cancellation, or timeout."""
            job_result = context["ti"].xcom_pull(
                task_ids="compute_engine.monitor_compute_job", key="job_result"
            )
            return validate_compute_execution(job_result=job_result)

        submit = _submit_compute_job()
        monitor = _monitor_compute_job(submit)
        _validate_compute_execution()

    # ═══════════════════════════════════════════════════════════════════════
    # TaskGroup: validation_and_metrics
    # ═══════════════════════════════════════════════════════════════════════
    with TaskGroup(group_id="validation_and_metrics") as validation_group:

        @task(
            task_id="read_compute_metrics",
            soft_fail=True,  # OPTIONAL — storage may be slow; quality_gate has empty-metrics fallback
            pool="{{ pipeline.airflow.pool }}",
        )
        def _read_compute_metrics(execution_result, **context):
            """OPTIONAL (soft_fail): Reads metrics.json from compute engine output path."""
            from platform.infrastructure.storage_reader import StorageReader
            return StorageReader().read_json(execution_result.get("metrics_path")) or {}

        @task(task_id="quality_gate", pool="{{ pipeline.airflow.pool }}")
        def _quality_gate(metrics, **context):
            """MANDATORY: Blocks load if quality rules are violated."""
            return quality_gate(
                pipeline_id="{{ pipeline.id }}",
                metrics=metrics or {},
                quality_rules=context["params"]["quality_rules"],
            )

        metrics_result = _read_compute_metrics(compute_engine_group)
        _quality_gate(metrics_result)

    # ═══════════════════════════════════════════════════════════════════════
    # TaskGroup: data_load — MANDATORY
    # ═══════════════════════════════════════════════════════════════════════
    with TaskGroup(group_id="data_load") as data_load_group:

        @task(task_id="load_to_data_warehouse", pool="{{ pipeline.airflow.pool }}")
        def _load_to_data_warehouse(execution_result, **context):
            """MANDATORY: Loads parquet from compute engine output into the data warehouse."""
            return load_to_data_warehouse(
                pipeline_id="{{ pipeline.id }}",
                destination_object_ids=context["params"]["destination_object_ids"],
                parquet_path=execution_result.get("output_path"),
                schema_path=execution_result.get("schema_path"),
            )

        @task(task_id="post_load_validation", pool="{{ pipeline.airflow.pool }}")
        def _post_load_validation(load_result, metrics, **context):
            """MANDATORY: Validates row count and checksum between source and DW."""
            return post_load_validation(
                pipeline_id="{{ pipeline.id }}",
                expected_rows=(metrics or {}).get("rows_written", 0),
                actual_rows=load_result.get("rows_loaded", 0),
                source_checksum=(metrics or {}).get("checksum"),
                destination_checksum=load_result.get("checksum"),
            )

        load = _load_to_data_warehouse(compute_engine_group)
        _post_load_validation(load, validation_group)

    # ═══════════════════════════════════════════════════════════════════════
    # TaskGroup: observability — all OPTIONAL (soft_fail=True)
    # ═══════════════════════════════════════════════════════════════════════
    with TaskGroup(group_id="observability") as observability_group:

        @task(
            task_id="emit_raw_lineage",
            soft_fail=True,  # OPTIONAL — lineage catalog may be unavailable
            pool="{{ pipeline.airflow.pool }}",
        )
        def _emit_raw_lineage(execution_result, **context):
            """OPTIONAL (soft_fail): Emits column-level lineage from schema.json."""
            from platform.infrastructure.platform_client import get_platform_client
            get_platform_client().emit_raw_lineage(
                pipeline_id="{{ pipeline.id }}",
                source_object_ids=[{% for obj in pipeline.source.objects %}"{{ obj.object_id }}", {% endfor %}],
                destination_object_ids=context["params"]["destination_object_ids"],
                schema_path=execution_result.get("schema_path"),
            )

        @task(
            task_id="emit_final_lineage",
            soft_fail=True,  # OPTIONAL — lineage catalog may be unavailable
            pool="{{ pipeline.airflow.pool }}",
        )
        def _emit_final_lineage(**context):
            """OPTIONAL (soft_fail): Updates freshness_status and final lineage in catalog."""
            from platform.infrastructure.platform_client import get_platform_client
            get_platform_client().update_freshness_status(
                pipeline_id="{{ pipeline.id }}",
                destination_object_ids=context["params"]["destination_object_ids"],
            )

        @task(
            task_id="emit_monitoring_and_sla",
            soft_fail=True,             # OPTIONAL — monitoring platform may be down
            trigger_rule="all_done",    # Always runs — even if mandatory tasks failed
            pool="{{ pipeline.airflow.pool }}",
        )
        def _emit_monitoring(**context):
            """
            OPTIONAL (soft_fail) + trigger_rule=all_done.
            Always executes to persist PipelineRun record (last_run_at, status).
            Determines final status from task instance states in DAG run context.
            """
            dag_run = context["dag_run"]
            ti = context["task_instance"]
            # Collect optional task failures for PipelineRun.optional_failures
            optional_task_ids = [
                "observability.emit_raw_lineage",
                "observability.emit_final_lineage",
                "observability.success_notification",
            ]
            optional_failures = [
                t_id for t_id in optional_task_ids
                if dag_run.get_task_instance(t_id) and
                   dag_run.get_task_instance(t_id).state in ("failed", "upstream_failed")
            ]
            # Determine overall status
            failed_tasks = [t for t in dag_run.get_task_instances()
                            if t.state == "failed" and t.task_id not in optional_task_ids]
            if any("quality_gate" in t.task_id for t in failed_tasks):
                final_status = "quality_failed"
                failed_task = "validation_and_metrics.quality_gate"
            elif failed_tasks:
                final_status = "failed"
                failed_task = failed_tasks[0].task_id
            elif optional_failures:
                final_status = "partial"
                failed_task = None
            else:
                final_status = "success"
                failed_task = None

            metrics = context["ti"].xcom_pull(
                task_ids="validation_and_metrics.read_compute_metrics"
            ) or {}
            emit_monitoring_and_sla(
                pipeline_id="{{ pipeline.id }}",
                pipeline_name="{{ pipeline.name }}",
                pipeline_type="ingestion",
                dag_run_id=str(dag_run.run_id),
                sla_minutes={{ pipeline.airflow.sla_minutes }},
                metrics=metrics,
                dag_run_start=str(dag_run.start_date),
                status=final_status,
                failed_task=failed_task,
                optional_failures=optional_failures,
                quality_violations=context["ti"].xcom_pull(
                    task_ids="validation_and_metrics.quality_gate", default=[]
                ) if final_status == "quality_failed" else [],
            )

        @task(
            task_id="success_notification",
            soft_fail=True,  # OPTIONAL — notification failure does not affect data
            pool="{{ pipeline.airflow.pool }}",
        )
        def _success_notification(**context):
            """OPTIONAL (soft_fail): Synchronous success notification to pipeline owner."""
            success_notification(
                pipeline_id="{{ pipeline.id }}",
                pipeline_name="{{ pipeline.name }}",
                owner="{{ pipeline.owner }}",
            )

        raw_lin = _emit_raw_lineage(compute_engine_group)
        final_lin = _emit_final_lineage()
        data_load_group >> final_lin
        monitoring = _emit_monitoring()
        [raw_lin, final_lin] >> monitoring
        notification = _success_notification()
        monitoring >> notification

    # ═══════════════════════════════════════════════════════════════════════
    # DAG-level wiring between TaskGroups
    # ═══════════════════════════════════════════════════════════════════════
{% if has_sensor %}
    source_readiness >> compute_engine_group
{% else %}
    pre_flight >> compute_engine_group
{% endif %}
    compute_engine_group >> validation_group
    validation_group >> data_load_group
    data_load_group >> observability_group


{{ pipeline.name | replace("-", "_") }}_dag()
```

- [ ] **Step 2: Atualizar etl_dag.py.j2 com TaskGroups**

```jinja2
{# platform/infrastructure/dag_generator/templates/etl_dag.py.j2 #}
{# Airflow 3 — pipeline.type = "etl"                              #}
{# 12 tasks in 4 TaskGroups                                       #}
{% from "_shared_macros.j2" import dag_imports, pipeline_asset, dag_schedule, dag_default_args %}
{{ dag_imports(pipeline) }}
from airflow.sdk import TaskGroup
from platform.infrastructure.airflow_callbacks.etl_callbacks import (
    validate_source_models,
    classify_schema_changes,
    submit_transformation_job,
    publish_documentation,
)

{{ pipeline_asset(pipeline) }}

@dag(
    dag_id="{{ pipeline.name }}",
    description="ETL pipeline — generated by platform. Do not edit manually.",
    {{ dag_schedule(pipeline) }}
    {{ dag_default_args(pipeline) }}
    tags={{ pipeline.airflow.tags | tojson }},
    catchup=False,
    max_active_runs=1,
    on_failure_callback=alert_and_monitoring,
    outlets=[_PIPELINE_ASSET],
)
def {{ pipeline.name | replace("-", "_") }}_dag():

    with TaskGroup(group_id="pre_flight") as pre_flight:
        @task(task_id="check_dependencies", pool="{{ pipeline.airflow.pool }}")
        def _check_dependencies(**context):
            """MANDATORY."""
            return check_dependencies(
                pipeline_id="{{ pipeline.id }}",
                depends_on={{ pipeline.schedule.depends_on | default([]) | tojson }},
                logical_date=context["logical_date"],
            )

        @task(task_id="validate_source_models", pool="{{ pipeline.airflow.pool }}")
        def _validate_source_models(**context):
            """MANDATORY: Validates dbt/Dataform models exist and are fresh."""
            return validate_source_models(
                pipeline_id="{{ pipeline.id }}",
                source_asset_id="{{ pipeline.source.asset_id }}",
            )

        @task(task_id="classify_schema_changes", pool="{{ pipeline.airflow.pool }}")
        def _classify_schema_changes(source_models, **context):
            """MANDATORY: Blocks if schema changes are incompatible with transformation."""
            return classify_schema_changes(source_models=source_models)

        _check_dependencies() >> (models := _validate_source_models())
        _classify_schema_changes(models)

    with TaskGroup(group_id="compute_engine") as compute_engine_group:
        @task(task_id="submit_transformation_job", pool="{{ pipeline.airflow.pool }}")
        def _submit_transformation_job(**context):
            """MANDATORY: Submits dbt or Dataform job asynchronously."""
            return submit_transformation_job(
                pipeline_id="{{ pipeline.id }}",
                transform_engine="{{ pipeline.transform.engine }}",
                transform_ref="{{ pipeline.transform.ref }}",
                compute_config={"engine": "{{ pipeline.compute.engine }}", "num_workers": {{ pipeline.compute.config.num_workers }}},
            )

        @task.sensor(
            task_id="monitor_transformation_job",
            mode="reschedule",
            timeout={{ pipeline.airflow.execution_timeout_minutes }} * 60,
            poke_interval=30,
            pool="{{ pipeline.airflow.pool }}",
        )
        def _monitor_transformation_job(submit_result, **context) -> bool:
            """MANDATORY: Polls transformation job until terminal."""
            from platform.infrastructure.compute_job_factory import get_transform_adapter
            adapter = get_transform_adapter("{{ pipeline.transform.engine }}")
            result = adapter.poll_job_status(submit_result["job_id"])
            context["ti"].xcom_push(key="job_result", value=result.__dict__)
            return result.status not in ("pending", "running")

        @task(task_id="validate_transformation_execution", pool="{{ pipeline.airflow.pool }}")
        def _validate_transformation_execution(**context):
            """MANDATORY: Raises if transformation job failed."""
            job_result = context["ti"].xcom_pull(task_ids="compute_engine.monitor_transformation_job", key="job_result")
            return validate_compute_execution(job_result=job_result)

        submit = _submit_transformation_job()
        monitor = _monitor_transformation_job(submit)
        _validate_transformation_execution()

    with TaskGroup(group_id="validation_and_metrics") as validation_group:
        @task(task_id="read_transformation_metrics", soft_fail=True, pool="{{ pipeline.airflow.pool }}")
        def _read_transformation_metrics(execution_result, **context):
            """OPTIONAL (soft_fail): Reads transformation metrics.json."""
            from platform.infrastructure.storage_reader import StorageReader
            return StorageReader().read_json(execution_result.get("metrics_path")) or {}

        @task(task_id="quality_gate", pool="{{ pipeline.airflow.pool }}")
        def _quality_gate(metrics, **context):
            """MANDATORY."""
            return quality_gate(
                pipeline_id="{{ pipeline.id }}",
                metrics=metrics or {},
                quality_rules={{ pipeline.quality.metrics | tojson }},
            )

        metrics_result = _read_transformation_metrics(compute_engine_group)
        _quality_gate(metrics_result)

    with TaskGroup(group_id="observability") as observability_group:
        @task(task_id="publish_documentation", soft_fail=True, pool="{{ pipeline.airflow.pool }}")
        def _publish_documentation(**context):
            """OPTIONAL (soft_fail): Publishes dbt/Dataform docs to catalog."""
            publish_documentation(pipeline_id="{{ pipeline.id }}", transform_ref="{{ pipeline.transform.ref }}")

        @task(task_id="emit_lineage", soft_fail=True, pool="{{ pipeline.airflow.pool }}")
        def _emit_lineage(execution_result, **context):
            """OPTIONAL (soft_fail): Emits transformation lineage to catalog."""
            from platform.infrastructure.platform_client import get_platform_client
            get_platform_client().emit_etl_lineage(
                pipeline_id="{{ pipeline.id }}",
                transform_ref="{{ pipeline.transform.ref }}",
                schema_path=execution_result.get("schema_path"),
            )

        @task(task_id="emit_monitoring_and_sla", soft_fail=True, trigger_rule="all_done", pool="{{ pipeline.airflow.pool }}")
        def _emit_monitoring(**context):
            """OPTIONAL (soft_fail) + trigger_rule=all_done. Persists PipelineRun."""
            metrics = context["ti"].xcom_pull(task_ids="validation_and_metrics.read_transformation_metrics") or {}
            emit_monitoring_and_sla(
                pipeline_id="{{ pipeline.id }}",
                pipeline_name="{{ pipeline.name }}",
                pipeline_type="etl",
                dag_run_id=str(context["dag_run"].run_id),
                sla_minutes={{ pipeline.airflow.sla_minutes }},
                metrics=metrics,
                dag_run_start=str(context["dag_run"].start_date),
                status="success",  # simplified — full logic as in ingestion template
                failed_task=None,
                optional_failures=[],
                quality_violations=[],
            )

        @task(task_id="success_notification", soft_fail=True, pool="{{ pipeline.airflow.pool }}")
        def _success_notification(**context):
            """OPTIONAL (soft_fail): Synchronous success notification."""
            success_notification(pipeline_id="{{ pipeline.id }}", pipeline_name="{{ pipeline.name }}", owner="{{ pipeline.owner }}")

        _publish_documentation() >> _emit_lineage(compute_engine_group)
        monitoring = _emit_monitoring()
        notification = _success_notification()
        monitoring >> notification

    pre_flight >> compute_engine_group >> validation_group >> observability_group


{{ pipeline.name | replace("-", "_") }}_dag()
```

- [ ] **Step 3: Atualizar export_dag.py.j2 com TaskGroups**

```jinja2
{# platform/infrastructure/dag_generator/templates/export_dag.py.j2 #}
{# Airflow 3 — pipeline.type = "export"                             #}
{# 14 tasks in 5 TaskGroups                                         #}
{% from "_shared_macros.j2" import dag_imports, pipeline_asset, dag_schedule, dag_default_args %}
{{ dag_imports(pipeline) }}
from airflow.sdk import TaskGroup
from platform.infrastructure.airflow_callbacks.export_callbacks import (
    validate_export_configuration,
    validate_source_dataset_readiness,
    classify_export_actions,
    publish_export_artifacts,
    validate_delivery,
)

{{ pipeline_asset(pipeline) }}

@dag(
    dag_id="{{ pipeline.name }}",
    description="Export pipeline — generated by platform. Do not edit manually.",
    {{ dag_schedule(pipeline) }}
    {{ dag_default_args(pipeline) }}
    tags={{ pipeline.airflow.tags | tojson }},
    catchup=False,
    max_active_runs=1,
    on_failure_callback=alert_and_monitoring,
    outlets=[_PIPELINE_ASSET],
)
def {{ pipeline.name | replace("-", "_") }}_dag():

    with TaskGroup(group_id="pre_flight") as pre_flight:
        @task(task_id="check_dependencies", pool="{{ pipeline.airflow.pool }}")
        def _check_dependencies(**context):
            """MANDATORY."""
            return check_dependencies(pipeline_id="{{ pipeline.id }}", depends_on={{ pipeline.schedule.depends_on | default([]) | tojson }}, logical_date=context["logical_date"])

        @task(task_id="validate_export_configuration", pool="{{ pipeline.airflow.pool }}")
        def _validate_export_config(**context):
            """MANDATORY: Invalid config → wrong delivery format or destination."""
            return validate_export_configuration(pipeline_id="{{ pipeline.id }}", destination_config={{ pipeline.destination | tojson }})

        @task(task_id="validate_source_dataset_readiness", pool="{{ pipeline.airflow.pool }}")
        def _validate_source_readiness(**context):
            """MANDATORY: STALE source → exporting outdated data."""
            return validate_source_dataset_readiness(pipeline_id="{{ pipeline.id }}", source_object_ids=[{% for obj in pipeline.source.objects %}"{{ obj.object_id }}", {% endfor %}])

        @task(task_id="classify_export_actions", pool="{{ pipeline.airflow.pool }}")
        def _classify_export_actions(export_config, source_readiness, **context):
            """MANDATORY: Determines export mode (full/incremental) and actions."""
            return classify_export_actions(pipeline_id="{{ pipeline.id }}", source_snapshot=source_readiness)

        deps = _check_dependencies()
        cfg = _validate_export_config()
        src = _validate_source_readiness()
        [deps, cfg, src] >> _classify_export_actions(cfg, src)

    with TaskGroup(group_id="compute_engine") as compute_engine_group:
        @task(task_id="submit_compute_export_job", pool="{{ pipeline.airflow.pool }}")
        def _submit_compute_export_job(export_actions, **context):
            """MANDATORY."""
            from platform.infrastructure.compute_job_factory import get_compute_adapter
            adapter = get_compute_adapter("{{ pipeline.compute.engine }}")
            job_id = adapter.submit_job(pipeline_id="{{ pipeline.id }}", pipeline_type="export", config={"actions": export_actions, "staging_bucket": "{{ pipeline.compute.staging_bucket }}"})
            from datetime import datetime, timezone
            return {"job_id": job_id, "submitted_at": datetime.now(tz=timezone.utc).isoformat()}

        @task.sensor(task_id="monitor_compute_export_job", mode="reschedule", timeout={{ pipeline.airflow.execution_timeout_minutes }} * 60, poke_interval=30, pool="{{ pipeline.airflow.pool }}")
        def _monitor_compute_export_job(submit_result, **context) -> bool:
            """MANDATORY: Polls export job until terminal."""
            from platform.infrastructure.compute_job_factory import get_compute_adapter
            result = get_compute_adapter("{{ pipeline.compute.engine }}").poll_job_status(submit_result["job_id"])
            context["ti"].xcom_push(key="job_result", value=result.__dict__)
            return result.status not in ("pending", "running")

        @task(task_id="validate_compute_execution", pool="{{ pipeline.airflow.pool }}")
        def _validate_compute_execution(**context):
            """MANDATORY."""
            return validate_compute_execution(job_result=context["ti"].xcom_pull(task_ids="compute_engine.monitor_compute_export_job", key="job_result"))

        submit = _submit_compute_export_job(pre_flight)
        monitor = _monitor_compute_export_job(submit)
        _validate_compute_execution()

    with TaskGroup(group_id="validation_and_metrics") as validation_group:
        @task(task_id="read_export_metrics", soft_fail=True, pool="{{ pipeline.airflow.pool }}")
        def _read_export_metrics(execution_result, **context):
            """OPTIONAL (soft_fail)."""
            from platform.infrastructure.storage_reader import StorageReader
            return StorageReader().read_json(execution_result.get("metrics_path")) or {}

        @task(task_id="quality_gate", pool="{{ pipeline.airflow.pool }}")
        def _quality_gate(metrics, **context):
            """MANDATORY."""
            return quality_gate(pipeline_id="{{ pipeline.id }}", metrics=metrics or {}, quality_rules={{ pipeline.quality.metrics | tojson }})

        metrics_result = _read_export_metrics(compute_engine_group)
        _quality_gate(metrics_result)

    with TaskGroup(group_id="delivery") as delivery_group:
        @task(task_id="publish_export_artifacts", pool="{{ pipeline.airflow.pool }}")
        def _publish_export_artifacts(execution_result, export_actions, **context):
            """MANDATORY: Delivers parquet/files to configured destination."""
            return publish_export_artifacts(pipeline_id="{{ pipeline.id }}", output_path=execution_result.get("output_path"), destination_config={{ pipeline.destination | tojson }})

        @task(task_id="validate_delivery", pool="{{ pipeline.airflow.pool }}")
        def _validate_delivery(delivery_result, **context):
            """MANDATORY: Incomplete delivery is a critical failure."""
            return validate_delivery(pipeline_id="{{ pipeline.id }}", delivery_result=delivery_result)

        artifacts = _publish_export_artifacts(compute_engine_group, pre_flight)
        _validate_delivery(artifacts)

    with TaskGroup(group_id="observability") as observability_group:
        @task(task_id="emit_export_lineage", soft_fail=True, pool="{{ pipeline.airflow.pool }}")
        def _emit_export_lineage(execution_result, **context):
            """OPTIONAL (soft_fail)."""
            from platform.infrastructure.platform_client import get_platform_client
            get_platform_client().emit_export_lineage(pipeline_id="{{ pipeline.id }}", source_object_ids=[{% for obj in pipeline.source.objects %}"{{ obj.object_id }}", {% endfor %}], destination_object_ids=[{% for obj in pipeline.destination.objects %}"{{ obj.object_id }}", {% endfor %}], schema_path=execution_result.get("schema_path"))

        @task(task_id="emit_monitoring_and_sla", soft_fail=True, trigger_rule="all_done", pool="{{ pipeline.airflow.pool }}")
        def _emit_monitoring(**context):
            """OPTIONAL (soft_fail) + trigger_rule=all_done. Persists PipelineRun."""
            metrics = context["ti"].xcom_pull(task_ids="validation_and_metrics.read_export_metrics") or {}
            emit_monitoring_and_sla(
                pipeline_id="{{ pipeline.id }}", pipeline_name="{{ pipeline.name }}", pipeline_type="export",
                dag_run_id=str(context["dag_run"].run_id), sla_minutes={{ pipeline.airflow.sla_minutes }},
                metrics=metrics, dag_run_start=str(context["dag_run"].start_date),
                status="success", failed_task=None, optional_failures=[], quality_violations=[],
            )

        @task(task_id="success_notification", soft_fail=True, pool="{{ pipeline.airflow.pool }}")
        def _success_notification(**context):
            """OPTIONAL (soft_fail): Synchronous success notification."""
            success_notification(pipeline_id="{{ pipeline.id }}", pipeline_name="{{ pipeline.name }}", owner="{{ pipeline.owner }}")

        _emit_export_lineage(compute_engine_group)
        monitoring = _emit_monitoring()
        monitoring >> _success_notification()

    pre_flight >> compute_engine_group >> validation_group >> delivery_group >> observability_group


{{ pipeline.name | replace("-", "_") }}_dag()
```

- [ ] **Step 4: Atualizar testes do DagGenerator**

```python
# tests/unit/infrastructure/test_dag_generator.py (adições v3)

def test_ingestion_dag_has_task_groups() -> None:
    dag_code = DagGenerator().generate(_yaml(_make_pipeline(PipelineType.INGESTION)))
    required_groups = [
        "pre_flight", "compute_engine", "validation_and_metrics",
        "data_load", "observability",
    ]
    for group in required_groups:
        assert f'group_id="{group}"' in dag_code, f"Missing TaskGroup: {group}"


def test_optional_tasks_have_soft_fail() -> None:
    dag_code = DagGenerator().generate(_yaml(_make_pipeline(PipelineType.INGESTION)))
    # Optional tasks must have soft_fail=True
    optional_tasks = ["read_compute_metrics", "emit_raw_lineage", "emit_final_lineage",
                      "emit_monitoring_and_sla", "success_notification"]
    for task_name in optional_tasks:
        # Each optional task definition block should appear before soft_fail=True
        assert "soft_fail=True" in dag_code, f"soft_fail=True missing for optional tasks"


def test_emit_monitoring_has_trigger_rule_all_done() -> None:
    dag_code = DagGenerator().generate(_yaml(_make_pipeline(PipelineType.INGESTION)))
    assert 'trigger_rule="all_done"' in dag_code


def test_all_dags_use_get_platform_client_not_direct_instantiation() -> None:
    """Ensure templates never call PlatformApiClient() directly."""
    for ptype in [PipelineType.INGESTION, PipelineType.ETL, PipelineType.EXPORT]:
        dag_code = DagGenerator().generate(_yaml(_make_pipeline(ptype)))
        assert "get_platform_client()" in dag_code
        assert "PlatformApiClient()" not in dag_code
```

- [ ] **Step 5: Rodar todos os testes**

```bash
uv run pytest tests/ -v
```

- [ ] **Step 6: Commit final**

```bash
git add .
git commit -m "feat: plan-02 v3 complete — TaskGroup classification (mandatory/optional), get_platform_client @cache, PipelineRun dashboard entity, sync notifications, soft_fail optional tasks"
```

---

## Task 12: CI Validator, Schema Migrator, YAML Generator Tests, CLI

*(Mantém conteúdo da v2 — adaptar SensorConfig separado e referências a `get_platform_client`)*

- [ ] **Step 1: Atualizar CiValidator para SensorConfig separado**

```python
# platform/infrastructure/dag_generator/ci_validator.py
# sensor_query em source.objects[].sensor.query
# sensor_query_timeout_minutes em source.objects[].sensor.timeout_minutes

def _validate_sensor_timeout(self, doc: dict) -> list[str]:
    errors = []
    exec_timeout = doc.get("pipeline", {}).get("airflow", {}).get("execution_timeout_minutes", 120)
    for obj in doc.get("pipeline", {}).get("source", {}).get("objects", []):
        sensor = obj.get("sensor") or {}
        if sensor.get("query"):
            sensor_timeout = sensor.get("timeout_minutes", 60)
            if sensor_timeout > exec_timeout:
                errors.append(
                    f"sensor.timeout_minutes ({sensor_timeout}) > "
                    f"execution_timeout_minutes ({exec_timeout}) "
                    f"for object_id={obj.get('object_id')!r}"
                )
    return errors
```

- [ ] **Step 2: Rodar CI gate completo**

```bash
make check
```

Esperado: `✅ All checks passed.`

- [ ] **Step 3: Commit final**

```bash
git add .
git commit -m "feat: plan-02 v3 complete — DataObject, DataElement, Pipeline, LineageMapping, PipelineRun, DAG Generator (Airflow 3 + TaskGroups), CI Validator, CLI"
```

---

## Self-Review v3

| Feedback | Implementação |
|---|---|
| **Mandatory vs Optional tasks** | Tabelas de classificação por tipo de DAG. OPTIONAL usam `@task(soft_fail=True)`. MANDATORY usam `trigger_rule` default. |
| **TaskGroup** | Ingestion: 6 grupos (`pre_flight`, `source_readiness`, `compute_engine`, `validation_and_metrics`, `data_load`, `observability`). ETL: 4 grupos. Export: 5 grupos. |
| **`get_platform_client()`** | Factory com `@lru_cache(maxsize=1)` em `platform/infrastructure/platform_client.py`. Todos os callbacks importam via esta função. Testes usam `get_platform_client.cache_clear()` para reset. |
| **Notificação síncrona** | `success_notification` usa `adapter.send_sync()` — sem `asyncio.run()`. |
| **`PipelineRun` entity** | `PipelineRunStatus` enum (running/success/failed/quality_failed/partial). `PipelineRun` domain entity com `mark_success()`, `mark_failed()`, `mark_quality_failed()`, `is_partial()`. |
| **`PipelineRunModel` ORM** | `last_run_at` (sempre), `last_success_at` (só em SUCCESS/PARTIAL), `failed_task`, `optional_failures`, `metrics`, `sla_breached`. Dashboard queries via `find_dashboard_summary()`. |
| **TaskGroup XCom paths** | Tasks dentro de TaskGroup usam `task_ids="group_id.task_id"` para `xcom_pull`. Ex: `"compute_engine.monitor_compute_job"`. |
| **`emit_monitoring_and_sla`** | Recebe `status`, `failed_task`, `optional_failures`, `quality_violations` para persistir `PipelineRun` completo. `trigger_rule=all_done` garante execução mesmo em falha. |

**Próximo plano:** `2026-06-28-plan-03-discovery-engine.md` — Discovery runners por endpoint type, PolicyTag inference, SchemaDrift detection (spec 4.2), DiscoveryRun aggregate.

