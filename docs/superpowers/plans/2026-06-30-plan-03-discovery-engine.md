# Data Platform — Discovery Engine (Plano 3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Dependências:** Plano 1 (DataAsset, Endpoint, RBAC, UoW), Plano 2 (DataObject, DataElement, Pipeline).

**Objetivo:** Implementar o motor de autodescoberta de metadados (`DiscoveryEngine`) com runners polimórficos por tipo de Endpoint, inferência de PolicyTags, detecção e classificação de schema drift, mecanismo de self-healing com fluxo de aprovação para mudanças críticas, e a entidade `DiscoveryRun` como agregado de auditoria.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy 2.x async, Pydantic v2, uv, ruff, mypy.

## Global Constraints

- **Clean Architecture**: runners são `infrastructure`, o motor de regras é `domain`. Nenhuma regra de negócio em routers ou models SQLAlchemy.
- **Polimorfismo**: `DiscoveryRunner` é um Protocol. Um runner por tipo de Endpoint (`Database`, `RestApi`, `Sftp`, `CloudBucket`, `EtlFlow`). Nenhum `if endpoint_type == "DATABASE"` fora da factory.
- **DiscoveryRun**: agregado de auditoria. Imutável após `completed`. Persistido via repositório próprio.
- **SchemaDrift**: classificação em `INFORMATIVE` vs `CRITICAL`. Mudanças críticas nunca auto-aprovadas — exigem `DriftApproval` explícita do owner do Asset.
- **PolicyTag inference**: sugestões com nível de confiança (`HIGH / MEDIUM / LOW`). Nunca aplicadas sem confirmação do owner.
- **get_platform_client()**: usado em todos os callbacks/adapters — jamais `PlatformApiClient()` direto.
- XCom carrega apenas referências externas (discovery_run_id, report_path). Nunca resultados completos.
- Code comments em inglês. Documentação `.md` em português.

---

## Task Classification

| Task (DiscoveryEngine execution) | Tipo | Justificativa |
|---|---|---|
| Conectar ao Endpoint | **MANDATORY** | Falha = impossível descobrir |
| Coletar metadados brutos | **MANDATORY** | Sem metadados = sem resultado |
| Detectar schema drift | **MANDATORY** | Drift não detectado = catálogo desatualizado silenciosamente |
| Classificar mudanças | **MANDATORY** | Classificação errada compromete ação automática |
| Inferir PolicyTags | **OPTIONAL** | Falha de inferência não bloqueia catálogo (sugestão, não decisão) |
| Gerar descrições auto-geradas | **OPTIONAL** | Side effect — falha não afeta metadados estruturais |
| Persistir DiscoveryRun | **MANDATORY** | Auditoria e idempotência dependem do registro |
| Notificar owner | **OPTIONAL** | Canal de notificação pode estar down |
| Aplicar self-healing (informativo) | **MANDATORY** | Schema deve ser atualizado automaticamente conforme política |
| Registrar DriftApproval (crítico) | **MANDATORY** | Mudança crítica deve aguardar aprovação — não pode ser ignorada |

---

## Estrutura de Arquivos

```
platform/
├── domain/
│   ├── discovery/                            # [NEW BOUNDED CONTEXT]
│   │   ├── discovery_run_status.py           # DiscoveryRunStatus enum
│   │   ├── drift_severity.py                 # DriftSeverity: INFORMATIVE | CRITICAL
│   │   ├── drift_change_type.py              # DriftChangeType: FIELD_ADDED | FIELD_REMOVED | ...
│   │   ├── policy_tag_confidence.py          # PolicyTagConfidence: HIGH | MEDIUM | LOW
│   │   ├── schema_snapshot.py                # SchemaSnapshot Value Object
│   │   ├── schema_field.py                   # SchemaField Value Object
│   │   ├── drift_event.py                    # DriftEvent Value Object (detected change)
│   │   ├── policy_tag_suggestion.py          # PolicyTagSuggestion Value Object
│   │   ├── discovery_run.py                  # DiscoveryRun aggregate root
│   │   ├── drift_approval.py                 # DriftApproval entity (approval/rejection of critical drift)
│   │   ├── discovery_run_repository.py       # DiscoveryRunRepository Protocol
│   │   └── drift_approval_repository.py      # DriftApprovalRepository Protocol
│   └── shared/
│       ├── auditable.py                      # (exists) Auditable mixin
│       └── secrets/                          # [NEW]
│           └── secret_resolver.py            # SecretResolver Protocol (domain/shared)
│
├── application/
│   ├── unit_of_work.py                       # [UPDATED] + discovery_runs, drift_approvals
│   └── discovery/
│       ├── platform_client.py                # PlatformClient Protocol (application boundary)
│       ├── run_discovery.py                  # RunDiscoveryUseCase (trigger + persist DiscoveryRun)
│       ├── approve_drift.py                  # ApproveDriftUseCase (owner approves critical changes)
│       └── reject_drift.py                   # RejectDriftUseCase (owner rejects critical changes)
│
└── infrastructure/
    ├── persistence/
    │   ├── models/
    │   │   ├── discovery_run_model.py        # [NEW]
    │   │   └── drift_approval_model.py       # [NEW]
    │   └── repositories/
    │       ├── sql_discovery_run_repository.py  # [NEW]
    │       └── sql_drift_approval_repository.py # [NEW]
    ├── discovery/
    │   ├── discovery_runner.py               # DiscoveryRunner Protocol (with object_id)
    │   ├── discovery_runner_factory.py       # factory: endpoint_type → DiscoveryRunner
    │   ├── runners/
    │   │   ├── database_runner.py            # DATABASE (Oracle, Postgres, MySQL via SQLAlchemy)
    │   │   ├── rest_api_runner.py            # REST_API (OpenAPI/Swagger introspection)
    │   │   ├── sftp_runner.py                # SFTP (paramiko, file listing + schema sampling)
    │   │   ├── cloud_bucket_runner.py        # CLOUD_BUCKET (GCS/S3/Azure — Parquet/CSV schema)
    │   │   └── etl_flow_runner.py            # ETL_FLOW (Fivetran/Airbyte API)
    │   ├── schema_differ.py                  # SchemaDiffer: diff two SchemaSnapshots → list[DriftEvent]
    │   ├── policy_tag_inferrer.py            # PolicyTagInferrer: field names + patterns → suggestions
    │   ├── description_generator.py          # DescriptionGenerator: field name + context → description
    │   └── discovery_engine.py               # DiscoveryEngine: orchestrates runner + differ + inferrer (supports object_id)
    └── http/
        ├── schemas/
        │   └── discovery_schemas.py          # Pydantic schemas for API
        └── routers/
            └── discovery_router.py           # /discovery/** endpoints

```

---

## Task 1: Domain — Enums e Value Objects

---

- [ ] **Step 1: Criar enums de domínio**

```python
# platform/domain/discovery/discovery_run_status.py
from __future__ import annotations
from enum import StrEnum


class DiscoveryRunStatus(StrEnum):
    """
    Lifecycle of a single DiscoveryRun execution.

    PENDING:    triggered, not yet running.
    RUNNING:    runner is collecting metadata from the endpoint.
    COMPLETED:  metadata collected, diff computed, suggestions generated.
    FAILED:     runner failed to connect or collect. See error_message.
    PARTIAL:    metadata collected but optional steps (PolicyTag inference,
                description generation) soft_failed.
    """
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"
```

```python
# platform/domain/discovery/drift_severity.py
from __future__ import annotations
from enum import StrEnum


class DriftSeverity(StrEnum):
    """
    Classification of a schema change detected during Discovery.

    INFORMATIVE: change is safe — platform may apply self-healing automatically
                 according to the asset's on_critical_change policy.
    CRITICAL:    change may break existing pipelines or violate data contracts.
                 Requires explicit DriftApproval from the asset owner before
                 any action is taken. Never auto-applied.
    """
    INFORMATIVE = "informative"
    CRITICAL = "critical"
```

```python
# platform/domain/discovery/drift_change_type.py
from __future__ import annotations
from enum import StrEnum


class DriftChangeType(StrEnum):
    """
    Specific type of schema change detected between two SchemaSnapshots.

    FIELD_ADDED:           new field appeared in source schema.
    FIELD_REMOVED:         field present in previous snapshot is now absent.
    TYPE_WIDENED:          type broadened (e.g. INT → BIGINT). Generally safe.
    TYPE_INCOMPATIBLE:     type changed incompatibly (e.g. STRING → INTEGER). Always CRITICAL.
    NULLABLE_TO_REQUIRED:  field became non-nullable. Always CRITICAL.
    REQUIRED_TO_NULLABLE:  field became nullable. INFORMATIVE.
    OBJECT_ADDED:          new table/file/endpoint appeared in scope.
    OBJECT_REMOVED:        object previously in scope has disappeared. CRITICAL.
    """
    FIELD_ADDED = "field_added"
    FIELD_REMOVED = "field_removed"
    TYPE_WIDENED = "type_widened"
    TYPE_INCOMPATIBLE = "type_incompatible"
    NULLABLE_TO_REQUIRED = "nullable_to_required"
    REQUIRED_TO_NULLABLE = "required_to_nullable"
    OBJECT_ADDED = "object_added"
    OBJECT_REMOVED = "object_removed"
```

```python
# platform/domain/discovery/policy_tag_confidence.py
from __future__ import annotations
from enum import StrEnum


class PolicyTagConfidence(StrEnum):
    """
    Confidence level of an inferred PolicyTag suggestion.

    HIGH:   strong semantic match (e.g. field named "cpf", "email", "senha").
            Owner should review and confirm quickly.
    MEDIUM: partial match or common abbreviation (e.g. "doc_num", "birth").
            Owner should evaluate context.
    LOW:    weak pattern match. Suggestion is informative only.
    """
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
```

- [ ] **Step 2: Criar Value Objects — SchemaField e SchemaSnapshot**

```python
# platform/domain/discovery/schema_field.py
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class SchemaField:
    """
    Immutable description of a single field/column within a DataObject schema.

    source_type is the raw type string from the endpoint (e.g. "VARCHAR2(255)", "INT4").
    normalized_type is the platform's canonical ElementType string after mapping.
    nullable defaults to True to be conservative — runners override when known.
    """

    name: str
    source_type: str                      # raw type from endpoint
    normalized_type: str                  # platform canonical (ElementType value)
    nullable: bool = True
    is_primary_key: bool = False
    description: str | None = None        # from source comments if available
    extra: dict = field(default_factory=dict)   # provider-specific metadata (precision, scale, etc.)

    def is_compatible_with(self, other: "SchemaField") -> bool:
        """
        True when this field's normalized_type is safely widened from other's.
        Compatible: INT → BIGINT, VARCHAR(50) → VARCHAR(255).
        Incompatible: STRING → INTEGER, DATE → TIMESTAMP (destructive).
        """
        WIDENING_MAP: dict[str, set[str]] = {
            "integer": {"bigint", "float", "decimal"},
            "bigint": {"float", "decimal"},
            "float": {"decimal"},
            "string": {"string"},       # length widening is OK within same base type
        }
        base_other = other.normalized_type.lower()
        base_self = self.normalized_type.lower()
        return base_self == base_other or base_self in WIDENING_MAP.get(base_other, set())
```

```python
# platform/domain/discovery/schema_snapshot.py
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass(frozen=True)
class SchemaSnapshot:
    """
    Immutable point-in-time snapshot of a DataObject schema as seen by the runner.

    object_id: the DataObject this snapshot belongs to.
    fields: ordered list of discovered fields.
    captured_at: UTC timestamp of when the runner collected this snapshot.
    runner_type: the EndpointType of the runner that produced this snapshot.
    row_count_estimate: optional — provided by runners that can do a lightweight count.
    """

    object_id: str
    fields: list["SchemaField"] = field(default_factory=list)
    captured_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    runner_type: str = ""                   # EndpointType value string
    object_name: str = ""                   # raw name in source (table, file, endpoint path)
    row_count_estimate: int | None = None

    def field_by_name(self, name: str) -> "SchemaField | None":
        return next((f for f in self.fields if f.name == name), None)

    def field_names(self) -> frozenset[str]:
        return frozenset(f.name for f in self.fields)

```

- [ ] **Step 3: Criar DriftEvent Value Object**

```python
# platform/domain/discovery/drift_event.py
from __future__ import annotations

from dataclasses import dataclass

from platform.domain.discovery.drift_change_type import DriftChangeType
from platform.domain.discovery.drift_severity import DriftSeverity


# Severity classification matrix — mirrors spec table 4.2 and data-asset-design 4.4
_SEVERITY_MAP: dict[DriftChangeType, DriftSeverity] = {
    DriftChangeType.FIELD_ADDED: DriftSeverity.INFORMATIVE,
    DriftChangeType.FIELD_REMOVED: DriftSeverity.CRITICAL,
    DriftChangeType.TYPE_WIDENED: DriftSeverity.INFORMATIVE,
    DriftChangeType.TYPE_INCOMPATIBLE: DriftSeverity.CRITICAL,
    DriftChangeType.NULLABLE_TO_REQUIRED: DriftSeverity.CRITICAL,
    DriftChangeType.REQUIRED_TO_NULLABLE: DriftSeverity.INFORMATIVE,
    DriftChangeType.OBJECT_ADDED: DriftSeverity.INFORMATIVE,
    DriftChangeType.OBJECT_REMOVED: DriftSeverity.CRITICAL,
}


@dataclass(frozen=True)
class DriftEvent:
    """
    Immutable record of a single detected schema change between two SchemaSnapshots.

    object_id: the DataObject where the change was detected.
    field_name: affected field (None for object-level changes like OBJECT_ADDED).
    change_type: the specific kind of structural change.
    severity: INFORMATIVE or CRITICAL — derived deterministically from change_type.
    previous_value: previous field type or None (for additions).
    current_value: current field type or None (for removals).
    description: human-readable summary for the owner notification and approval UI.
    """

    object_id: str
    change_type: DriftChangeType
    description: str
    field_name: str | None = None
    previous_value: str | None = None
    current_value: str | None = None

    @property
    def severity(self) -> DriftSeverity:
        """Severity is deterministic — derived from change_type, never overridable."""
        return _SEVERITY_MAP[self.change_type]

    @property
    def is_critical(self) -> bool:
        return self.severity == DriftSeverity.CRITICAL

    @property
    def requires_approval(self) -> bool:
        """True for all CRITICAL events. Platform never auto-applies critical drift."""
        return self.is_critical
```

- [ ] **Step 4: Criar PolicyTagSuggestion Value Object**

```python
# platform/domain/discovery/policy_tag_suggestion.py
from __future__ import annotations

from dataclasses import dataclass

from platform.domain.shared.policy_tag import PolicyTag
from platform.domain.discovery.policy_tag_confidence import PolicyTagConfidence


@dataclass(frozen=True)
class PolicyTagSuggestion:
    """
    Inferred PolicyTag suggestion for a DataElement field.

    Suggestions are NEVER applied automatically — they require explicit
    confirmation from the asset owner (PO/PM/AE) via the catalog UI or API.

    field_name: the DataElement field this suggestion targets.
    suggested_tag: the PolicyTag the inferrer believes applies.
    confidence: HIGH/MEDIUM/LOW — how strong the evidence is.
    matched_pattern: the pattern or rule that triggered the suggestion
                     (for owner review transparency).
    auto_generated_description: optional description suggested for the field.
    """

    field_name: str
    suggested_tag: PolicyTag
    confidence: PolicyTagConfidence
    matched_pattern: str                   # e.g. "field_name contains 'cpf'"
    auto_generated_description: str | None = None
```

- [ ] **Step 5: Testes dos Value Objects**

```python
# tests/unit/domain/discovery/test_drift_event.py
from __future__ import annotations

import pytest

from platform.domain.discovery.drift_change_type import DriftChangeType
from platform.domain.discovery.drift_event import DriftEvent
from platform.domain.discovery.drift_severity import DriftSeverity


@pytest.mark.parametrize("change_type,expected_severity", [
    (DriftChangeType.FIELD_ADDED, DriftSeverity.INFORMATIVE),
    (DriftChangeType.FIELD_REMOVED, DriftSeverity.CRITICAL),
    (DriftChangeType.TYPE_WIDENED, DriftSeverity.INFORMATIVE),
    (DriftChangeType.TYPE_INCOMPATIBLE, DriftSeverity.CRITICAL),
    (DriftChangeType.NULLABLE_TO_REQUIRED, DriftSeverity.CRITICAL),
    (DriftChangeType.REQUIRED_TO_NULLABLE, DriftSeverity.INFORMATIVE),
    (DriftChangeType.OBJECT_ADDED, DriftSeverity.INFORMATIVE),
    (DriftChangeType.OBJECT_REMOVED, DriftSeverity.CRITICAL),
])
def test_drift_event_severity_is_deterministic(
    change_type: DriftChangeType,
    expected_severity: DriftSeverity,
) -> None:
    event = DriftEvent(object_id="obj-1", change_type=change_type, description="test")
    assert event.severity == expected_severity


def test_critical_event_requires_approval() -> None:
    event = DriftEvent(
        object_id="obj-1",
        change_type=DriftChangeType.TYPE_INCOMPATIBLE,
        description="STRING → INTEGER",
    )
    assert event.is_critical is True
    assert event.requires_approval is True


def test_informative_event_does_not_require_approval() -> None:
    event = DriftEvent(
        object_id="obj-1",
        change_type=DriftChangeType.FIELD_ADDED,
        description="new field added",
    )
    assert event.is_critical is False
    assert event.requires_approval is False


def test_schema_field_compatible_with_same_type() -> None:
    from platform.domain.discovery.schema_field import SchemaField
    f1 = SchemaField(name="id", source_type="INT", normalized_type="integer")
    f2 = SchemaField(name="id", source_type="INT", normalized_type="integer")
    assert f2.is_compatible_with(f1) is True


def test_schema_field_widening_is_compatible() -> None:
    from platform.domain.discovery.schema_field import SchemaField
    f_old = SchemaField(name="id", source_type="INT", normalized_type="integer")
    f_new = SchemaField(name="id", source_type="BIGINT", normalized_type="bigint")
    assert f_new.is_compatible_with(f_old) is True


def test_schema_field_incompatible_type_change() -> None:
    from platform.domain.discovery.schema_field import SchemaField
    f_old = SchemaField(name="code", source_type="VARCHAR", normalized_type="string")
    f_new = SchemaField(name="code", source_type="INT", normalized_type="integer")
    assert f_new.is_compatible_with(f_old) is False
```

- [ ] **Step 6: Commit**

```bash
uv run pytest tests/unit/domain/discovery/test_drift_event.py -v
git add platform/domain/discovery/ tests/unit/domain/discovery/
git commit -m "feat: add Discovery domain enums and value objects (DriftEvent, SchemaSnapshot, PolicyTagSuggestion)"
```

---

## Task 2: Domain — DiscoveryRun Aggregate + DriftApproval

---

- [ ] **Step 1: Criar DiscoveryRun aggregate root**

```python
# platform/domain/discovery/discovery_run.py
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from platform.domain.discovery.discovery_run_status import DiscoveryRunStatus
from platform.domain.discovery.drift_event import DriftEvent
from platform.domain.discovery.policy_tag_suggestion import PolicyTagSuggestion
from platform.domain.discovery.schema_snapshot import SchemaSnapshot
from platform.domain.shared.auditable import Auditable


@dataclass
class DiscoveryRun(Auditable):
    """
    Aggregate root for a single Discovery execution against a DataAsset.

    Immutable after status transitions to COMPLETED or FAILED.
    Records the full result of the discovery: snapshots, drift events,
    PolicyTag suggestions, and optional descriptions.

    Lifecycle:
        PENDING → RUNNING → COMPLETED (or FAILED / PARTIAL)

    The DiscoveryRun is the single source of truth for:
    - What the schema looked like at a point in time (SchemaSnapshot per object).
    - What changed vs the previous run (DriftEvent list).
    - What PolicyTag suggestions were generated (PolicyTagSuggestion list).
    - Whether any critical drift requires owner approval.
    """

    id: str
    asset_id: str                                         # DataAsset being discovered
    triggered_by: str                                     # "schedule" | "manual" | "initial"
    status: DiscoveryRunStatus = DiscoveryRunStatus.PENDING
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error_message: str | None = None
    snapshots: list[SchemaSnapshot] = field(default_factory=list)
    drift_events: list[DriftEvent] = field(default_factory=list)
    policy_tag_suggestions: list[PolicyTagSuggestion] = field(default_factory=list)
    auto_generated_descriptions: dict[str, str] = field(default_factory=dict)  # field_name → description
    objects_discovered: int = 0
    fields_discovered: int = 0
    # Optional failures (soft_fail equivalents — do not block core discovery)
    soft_failures: list[str] = field(default_factory=list)

    # ── lifecycle transitions ──────────────────────────────────────────────

    def start(self) -> None:
        """Transition from PENDING to RUNNING."""
        if self.status != DiscoveryRunStatus.PENDING:
            raise ValueError(f"Cannot start a run in status={self.status!r}")
        self.status = DiscoveryRunStatus.RUNNING
        self.started_at = datetime.now(tz=timezone.utc)
        self.touch()

    def complete(
        self,
        snapshots: list[SchemaSnapshot],
        drift_events: list[DriftEvent],
        policy_tag_suggestions: list[PolicyTagSuggestion],
        auto_generated_descriptions: dict[str, str],
        soft_failures: list[str] | None = None,
    ) -> None:
        """
        Transition from RUNNING to COMPLETED (or PARTIAL if soft_failures exist).
        Stores full discovery results.
        """
        if self.status != DiscoveryRunStatus.RUNNING:
            raise ValueError(f"Cannot complete a run in status={self.status!r}")
        self.snapshots = snapshots
        self.drift_events = drift_events
        self.policy_tag_suggestions = policy_tag_suggestions
        self.auto_generated_descriptions = auto_generated_descriptions
        self.soft_failures = soft_failures or []
        self.objects_discovered = len(snapshots)
        self.fields_discovered = sum(len(s.fields) for s in snapshots)
        self.completed_at = datetime.now(tz=timezone.utc)
        self.status = (
            DiscoveryRunStatus.PARTIAL if self.soft_failures else DiscoveryRunStatus.COMPLETED
        )
        self.touch()

    def fail(self, error_message: str) -> None:
        """Transition from RUNNING to FAILED. Records the error message."""
        if self.status != DiscoveryRunStatus.RUNNING:
            raise ValueError(f"Cannot fail a run in status={self.status!r}")
        self.status = DiscoveryRunStatus.FAILED
        self.error_message = error_message
        self.completed_at = datetime.now(tz=timezone.utc)
        self.touch()

    # ── query helpers ──────────────────────────────────────────────────────

    @property
    def has_critical_drift(self) -> bool:
        return any(e.is_critical for e in self.drift_events)

    @property
    def critical_events(self) -> list[DriftEvent]:
        return [e for e in self.drift_events if e.is_critical]

    @property
    def informative_events(self) -> list[DriftEvent]:
        return [e for e in self.drift_events if not e.is_critical]

    @property
    def high_confidence_suggestions(self) -> list[PolicyTagSuggestion]:
        from platform.domain.discovery.policy_tag_confidence import PolicyTagConfidence
        return [s for s in self.policy_tag_suggestions if s.confidence == PolicyTagConfidence.HIGH]

    def duration_seconds(self) -> float | None:
        if self.started_at is None or self.completed_at is None:
            return None
        return (self.completed_at - self.started_at).total_seconds()

    def is_terminal(self) -> bool:
        return self.status in (
            DiscoveryRunStatus.COMPLETED,
            DiscoveryRunStatus.FAILED,
            DiscoveryRunStatus.PARTIAL,
        )
```

- [ ] **Step 2: Criar DriftApproval entity**

```python
# platform/domain/discovery/drift_approval.py
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum

from platform.domain.discovery.drift_change_type import DriftChangeType
from platform.domain.shared.auditable import Auditable


class DriftApprovalDecision(StrEnum):
    """
    Decision made by the asset owner on a critical DriftEvent.

    APPROVED: owner accepts the change. Platform applies self-healing action.
    REJECTED: owner rejects the change. Pipelines depending on the affected
              DataObject are paused pending manual intervention.
    """
    APPROVED = "approved"
    REJECTED = "rejected"
    PENDING = "pending"


@dataclass
class DriftApproval(Auditable):
    """
    Records the asset owner's decision on a critical DriftEvent.

    Created automatically when a DiscoveryRun completes with CRITICAL drift.
    Transitions from PENDING to APPROVED or REJECTED via ApproveDrift/RejectDrift
    use cases — only the asset owner can act.

    One DriftApproval per DriftEvent per DiscoveryRun.
    A single DiscoveryRun may generate N DriftApprovals.
    """

    id: str
    discovery_run_id: str
    asset_id: str
    object_id: str
    field_name: str | None                        # None for object-level events
    change_type: DriftChangeType
    severity_description: str                     # human-readable change description
    decision: DriftApprovalDecision = DriftApprovalDecision.PENDING
    decided_by: str | None = None                 # user id of the owner who decided
    decided_at: datetime | None = None
    owner_notes: str | None = None                # optional notes from the owner

    def approve(self, decided_by: str, notes: str | None = None) -> None:
        """Owner approves the critical drift. Platform may apply self-healing."""
        if self.decision != DriftApprovalDecision.PENDING:
            raise ValueError(f"DriftApproval already decided: {self.decision!r}")
        self.decision = DriftApprovalDecision.APPROVED
        self.decided_by = decided_by
        self.decided_at = datetime.now(tz=timezone.utc)
        self.owner_notes = notes
        self.touch()

    def reject(self, decided_by: str, notes: str | None = None) -> None:
        """Owner rejects the critical drift. Affected pipelines are paused."""
        if self.decision != DriftApprovalDecision.PENDING:
            raise ValueError(f"DriftApproval already decided: {self.decision!r}")
        self.decision = DriftApprovalDecision.REJECTED
        self.decided_by = decided_by
        self.decided_at = datetime.now(tz=timezone.utc)
        self.owner_notes = notes
        self.touch()

    @property
    def is_pending(self) -> bool:
        return self.decision == DriftApprovalDecision.PENDING
```

- [ ] **Step 3: Criar Repositories (Protocols)**

```python
# platform/domain/discovery/discovery_run_repository.py
from __future__ import annotations
from typing import Protocol, runtime_checkable

from platform.domain.discovery.discovery_run import DiscoveryRun
from platform.domain.discovery.discovery_run_status import DiscoveryRunStatus


@runtime_checkable
class DiscoveryRunRepository(Protocol):

    async def save(self, run: DiscoveryRun) -> DiscoveryRun: ...

    async def find_by_id(self, run_id: str) -> DiscoveryRun | None: ...

    async def find_latest_by_asset_id(self, asset_id: str) -> DiscoveryRun | None:
        """Most recent terminal run for dashboard/diff baseline."""
        ...

    async def find_all_by_asset_id(
        self, asset_id: str, *, limit: int = 20
    ) -> list[DiscoveryRun]: ...
```

```python
# platform/domain/discovery/drift_approval_repository.py
from __future__ import annotations
from typing import Protocol, runtime_checkable

from platform.domain.discovery.drift_approval import DriftApproval, DriftApprovalDecision


@runtime_checkable
class DriftApprovalRepository(Protocol):

    async def save(self, approval: DriftApproval) -> DriftApproval: ...

    async def find_by_id(self, approval_id: str) -> DriftApproval | None: ...

    async def find_pending_by_asset_id(self, asset_id: str) -> list[DriftApproval]:
        """Return all pending approvals blocking self-healing for an asset."""
        ...

    async def find_by_discovery_run_id(
        self, discovery_run_id: str
    ) -> list[DriftApproval]: ...
```

- [ ] **Step 4: Testes do DiscoveryRun e DriftApproval**

```python
# tests/unit/domain/discovery/test_discovery_run.py
from __future__ import annotations

import uuid
from datetime import datetime, timezone
import pytest

from platform.domain.discovery.discovery_run import DiscoveryRun
from platform.domain.discovery.discovery_run_status import DiscoveryRunStatus
from platform.domain.discovery.drift_change_type import DriftChangeType
from platform.domain.discovery.drift_event import DriftEvent
from platform.domain.discovery.schema_snapshot import SchemaSnapshot


def _run(**kwargs) -> DiscoveryRun:
    return DiscoveryRun(id=str(uuid.uuid4()), asset_id="asset-1",
                        triggered_by="manual", **kwargs)


def test_start_transitions_to_running() -> None:
    run = _run()
    run.start()
    assert run.status == DiscoveryRunStatus.RUNNING
    assert run.started_at is not None


def test_start_from_non_pending_raises() -> None:
    run = _run()
    run.start()
    with pytest.raises(ValueError, match="Cannot start"):
        run.start()


def test_complete_with_no_soft_failures_is_completed() -> None:
    run = _run()
    run.start()
    run.complete(snapshots=[], drift_events=[], policy_tag_suggestions={}, auto_generated_descriptions={})
    assert run.status == DiscoveryRunStatus.COMPLETED
    assert run.completed_at is not None


def test_complete_with_soft_failures_is_partial() -> None:
    run = _run()
    run.start()
    run.complete(
        snapshots=[], drift_events=[], policy_tag_suggestions={},
        auto_generated_descriptions={},
        soft_failures=["PolicyTagInferrer: connection timeout"],
    )
    assert run.status == DiscoveryRunStatus.PARTIAL


def test_fail_records_error() -> None:
    run = _run()
    run.start()
    run.fail("Connection refused to endpoint")
    assert run.status == DiscoveryRunStatus.FAILED
    assert run.error_message == "Connection refused to endpoint"


def test_has_critical_drift_detects_correctly() -> None:
    run = _run()
    run.start()
    events = [
        DriftEvent(object_id="obj-1", change_type=DriftChangeType.TYPE_INCOMPATIBLE,
                   description="STRING → INTEGER"),
    ]
    run.complete(snapshots=[], drift_events=events, policy_tag_suggestions={},
                 auto_generated_descriptions={})
    assert run.has_critical_drift is True
    assert len(run.critical_events) == 1
    assert len(run.informative_events) == 0


def test_duration_seconds_computed_correctly() -> None:
    run = _run()
    run.start()
    from datetime import timedelta
    run.complete(snapshots=[], drift_events=[], policy_tag_suggestions={},
                 auto_generated_descriptions={})
    # duration should be >= 0 (near-instant in test)
    assert run.duration_seconds() is not None
    assert run.duration_seconds() >= 0.0


# tests/unit/domain/discovery/test_drift_approval.py
from __future__ import annotations

import uuid
import pytest

from platform.domain.discovery.drift_approval import DriftApproval, DriftApprovalDecision
from platform.domain.discovery.drift_change_type import DriftChangeType


def _approval(**kwargs) -> DriftApproval:
    return DriftApproval(
        id=str(uuid.uuid4()),
        discovery_run_id="run-1",
        asset_id="asset-1",
        object_id="obj-1",
        field_name="cpf",
        change_type=DriftChangeType.TYPE_INCOMPATIBLE,
        severity_description="cpf changed from STRING to INTEGER",
        **kwargs,
    )


def test_approve_transitions_decision() -> None:
    a = _approval()
    a.approve(decided_by="owner@company.com", notes="Confirmed safe migration")
    assert a.decision == DriftApprovalDecision.APPROVED
    assert a.decided_by == "owner@company.com"
    assert a.decided_at is not None


def test_reject_transitions_decision() -> None:
    a = _approval()
    a.reject(decided_by="owner@company.com")
    assert a.decision == DriftApprovalDecision.REJECTED
    assert a.is_pending is False


def test_double_decide_raises() -> None:
    a = _approval()
    a.approve(decided_by="owner@company.com")
    with pytest.raises(ValueError, match="already decided"):
        a.approve(decided_by="owner@company.com")
```

# platform/domain/shared/secrets/secret_resolver.py
from __future__ import annotations

from typing import Any, Protocol


class SecretResolver(Protocol):
    """
    Protocol for resolving credentials from Vault, AWS Secrets Manager,
    GCP Secret Manager or environment values without coupling the domain or
    infrastructure modules to a concrete key vault driver.
    """

    def resolve(self, secret_ref: str) -> dict[str, Any]:
        """
        Retrieve and decrypt the credentials dict by reference.
        Returns e.g. {"username": "foo", "password": "bar"} or {"token": "..."}.
        """
        ...
```

- [ ] **Step 5: Commit**

```bash
uv run pytest tests/unit/domain/discovery/ -v
git add platform/domain/discovery/ tests/unit/domain/discovery/ platform/domain/shared/secrets/
git commit -m "feat: add DiscoveryRun aggregate (lifecycle transitions), DriftApproval entity, and SecretResolver Protocol"
```


---

## Task 3: Infrastructure — ORM Models + Repositories

---

- [ ] **Step 1: Criar DiscoveryRunModel**

```python
# platform/infrastructure/persistence/models/discovery_run_model.py
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, Integer, JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from platform.infrastructure.persistence.base_model import Base, TimestampMixin


class DiscoveryRunModel(Base, TimestampMixin):
    """
    ORM model for DiscoveryRun aggregates.

    snapshots_json: list[SchemaSnapshot] serialized as JSON.
    drift_events_json: list[DriftEvent] serialized as JSON.
    policy_suggestions_json: list[PolicyTagSuggestion] serialized as JSON.
    auto_descriptions_json: dict[field_name, description] serialized as JSON.

    JSON fields are intentionally denormalized for simplicity at this phase.
    Phase 2 (Future): normalize SchemaSnapshot into a dedicated schema_fields table
    for queryable field-level history.
    """

    __tablename__ = "discovery_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    asset_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    triggered_by: Mapped[str] = mapped_column(String(50), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_message: Mapped[str | None] = mapped_column(String(2000), nullable=True)
    objects_discovered: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    fields_discovered: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    snapshots_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    drift_events_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    policy_suggestions_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    auto_descriptions_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    soft_failures: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
```

- [ ] **Step 2: Criar DriftApprovalModel**

```python
# platform/infrastructure/persistence/models/drift_approval_model.py
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from platform.infrastructure.persistence.base_model import Base, TimestampMixin


class DriftApprovalModel(Base, TimestampMixin):
    """
    ORM model for DriftApproval entities.

    One row per critical DriftEvent per DiscoveryRun.
    decision defaults to "pending" — updated by approve/reject use cases.
    """

    __tablename__ = "drift_approvals"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    discovery_run_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("discovery_runs.id"), nullable=False, index=True
    )
    asset_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    object_id: Mapped[str] = mapped_column(String(36), nullable=False)
    field_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    change_type: Mapped[str] = mapped_column(String(50), nullable=False)
    severity_description: Mapped[str] = mapped_column(String(2000), nullable=False)
    decision: Mapped[str] = mapped_column(String(30), nullable=False, default="pending")
    decided_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    owner_notes: Mapped[str | None] = mapped_column(String(2000), nullable=True)
```

- [ ] **Step 3: Criar SqlDiscoveryRunRepository**

```python
# platform/infrastructure/persistence/repositories/sql_discovery_run_repository.py
from __future__ import annotations

from dataclasses import asdict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from platform.domain.discovery.discovery_run import DiscoveryRun
from platform.domain.discovery.discovery_run_status import DiscoveryRunStatus
from platform.domain.discovery.drift_event import DriftEvent
from platform.domain.discovery.drift_change_type import DriftChangeType
from platform.domain.discovery.schema_snapshot import SchemaSnapshot
from platform.domain.discovery.schema_field import SchemaField
from platform.domain.discovery.policy_tag_suggestion import PolicyTagSuggestion
from platform.domain.discovery.policy_tag_confidence import PolicyTagConfidence
from platform.domain.shared.policy_tag import PolicyTag
from platform.infrastructure.persistence.models.discovery_run_model import DiscoveryRunModel


def _snapshot_from_dict(d: dict) -> SchemaSnapshot:
    from datetime import datetime, timezone
    return SchemaSnapshot(
        object_id=d["object_id"],
        object_name=d.get("object_name", ""),
        runner_type=d.get("runner_type", ""),
        captured_at=datetime.fromisoformat(d["captured_at"]),
        row_count_estimate=d.get("row_count_estimate"),
        fields=[
            SchemaField(
                name=f["name"],
                source_type=f["source_type"],
                normalized_type=f["normalized_type"],
                nullable=f.get("nullable", True),
                is_primary_key=f.get("is_primary_key", False),
                description=f.get("description"),
                extra=f.get("extra", {}),
            )
            for f in d.get("fields", [])
        ],
    )


def _snapshot_to_dict(s: SchemaSnapshot) -> dict:
    return {
        "object_id": s.object_id,
        "object_name": s.object_name,
        "runner_type": s.runner_type,
        "captured_at": s.captured_at.isoformat(),
        "row_count_estimate": s.row_count_estimate,
        "fields": [
            {
                "name": f.name,
                "source_type": f.source_type,
                "normalized_type": f.normalized_type,
                "nullable": f.nullable,
                "is_primary_key": f.is_primary_key,
                "description": f.description,
                "extra": f.extra,
            }
            for f in s.fields
        ],
    }


def _to_domain(m: DiscoveryRunModel) -> DiscoveryRun:
    run = DiscoveryRun(
        id=m.id,
        asset_id=m.asset_id,
        triggered_by=m.triggered_by,
        status=DiscoveryRunStatus(m.status),
        started_at=m.started_at,
        completed_at=m.completed_at,
        error_message=m.error_message,
        objects_discovered=m.objects_discovered,
        fields_discovered=m.fields_discovered,
        soft_failures=m.soft_failures,
        created_at=m.created_at,
        updated_at=m.updated_at,
    )
    run.snapshots = [_snapshot_from_dict(d) for d in (m.snapshots_json or [])]
    run.drift_events = [
        DriftEvent(
            object_id=e["object_id"],
            change_type=DriftChangeType(e["change_type"]),
            description=e["description"],
            field_name=e.get("field_name"),
            previous_value=e.get("previous_value"),
            current_value=e.get("current_value"),
        )
        for e in (m.drift_events_json or [])
    ]
    run.policy_tag_suggestions = [
        PolicyTagSuggestion(
            field_name=s["field_name"],
            suggested_tag=PolicyTag(s["suggested_tag"]),
            confidence=PolicyTagConfidence(s["confidence"]),
            matched_pattern=s["matched_pattern"],
            auto_generated_description=s.get("auto_generated_description"),
        )
        for s in (m.policy_suggestions_json or [])
    ]
    run.auto_generated_descriptions = m.auto_descriptions_json or {}
    return run


class SqlDiscoveryRunRepository:

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, run: DiscoveryRun) -> DiscoveryRun:
        model = DiscoveryRunModel(
            id=run.id,
            asset_id=run.asset_id,
            triggered_by=run.triggered_by,
            status=run.status.value,
            started_at=run.started_at,
            completed_at=run.completed_at,
            error_message=run.error_message,
            objects_discovered=run.objects_discovered,
            fields_discovered=run.fields_discovered,
            soft_failures=run.soft_failures,
            snapshots_json=[_snapshot_to_dict(s) for s in run.snapshots],
            drift_events_json=[
                {
                    "object_id": e.object_id,
                    "change_type": e.change_type.value,
                    "description": e.description,
                    "field_name": e.field_name,
                    "previous_value": e.previous_value,
                    "current_value": e.current_value,
                }
                for e in run.drift_events
            ],
            policy_suggestions_json=[
                {
                    "field_name": s.field_name,
                    "suggested_tag": s.suggested_tag.value,
                    "confidence": s.confidence.value,
                    "matched_pattern": s.matched_pattern,
                    "auto_generated_description": s.auto_generated_description,
                }
                for s in run.policy_tag_suggestions
            ],
            auto_descriptions_json=run.auto_generated_descriptions,
        )
        self._session.add(model)
        await self._session.flush()
        return _to_domain(model)

    async def find_by_id(self, run_id: str) -> DiscoveryRun | None:
        result = await self._session.execute(
            select(DiscoveryRunModel).where(DiscoveryRunModel.id == run_id)
        )
        m = result.scalar_one_or_none()
        return _to_domain(m) if m else None

    async def find_latest_by_asset_id(self, asset_id: str) -> DiscoveryRun | None:
        result = await self._session.execute(
            select(DiscoveryRunModel)
            .where(DiscoveryRunModel.asset_id == asset_id)
            .where(DiscoveryRunModel.status.in_(["completed", "partial"]))
            .order_by(DiscoveryRunModel.completed_at.desc())
            .limit(1)
        )
        m = result.scalar_one_or_none()
        return _to_domain(m) if m else None

    async def find_all_by_asset_id(
        self, asset_id: str, *, limit: int = 20
    ) -> list[DiscoveryRun]:
        result = await self._session.execute(
            select(DiscoveryRunModel)
            .where(DiscoveryRunModel.asset_id == asset_id)
            .order_by(DiscoveryRunModel.created_at.desc())
            .limit(limit)
        )
        return [_to_domain(m) for m in result.scalars().all()]
```

- [ ] **Step 4: Gerar migrations, commit**

```bash
make migrate-create name="add_discovery_runs_and_drift_approvals"
make migrate
git add platform/infrastructure/persistence/models/discovery_run_model.py \
        platform/infrastructure/persistence/models/drift_approval_model.py \
        platform/infrastructure/persistence/repositories/sql_discovery_run_repository.py \
        platform/infrastructure/persistence/repositories/sql_drift_approval_repository.py \
        migrations/
git commit -m "feat: add DiscoveryRun and DriftApproval ORM models + repositories"
```

---

## Task 4: Infrastructure — SchemaDiffer

**Propósito:** Confronta dois `SchemaSnapshot` (previous vs current) e produz a lista de `DriftEvent`. Puro Python — zero dependências externas.

**Regras de classificação** (spec 4.2 + data-asset-design 4.4):
| Detecção | DriftChangeType | Severity |
|---|---|---|
| Campo presente em current, ausente em previous | FIELD_ADDED | INFORMATIVE |
| Campo ausente em current, presente em previous | FIELD_REMOVED | CRITICAL |
| Mesmo campo, tipo compatível (widening) | TYPE_WIDENED | INFORMATIVE |
| Mesmo campo, tipo incompatível | TYPE_INCOMPATIBLE | CRITICAL |
| Campo ficou `nullable=False` (era True) | NULLABLE_TO_REQUIRED | CRITICAL |
| Campo ficou `nullable=True` (era False) | REQUIRED_TO_NULLABLE | INFORMATIVE |
| Objeto ausente em previous, presente em current | OBJECT_ADDED | INFORMATIVE |
| Objeto presente em previous, ausente em current | OBJECT_REMOVED | CRITICAL |

---

- [ ] **Step 1: Implementar SchemaDiffer**

```python
# platform/domain/discovery/services/schema_differ.py
from __future__ import annotations

from platform.domain.discovery.drift_change_type import DriftChangeType
from platform.domain.discovery.drift_event import DriftEvent
from platform.domain.discovery.schema_snapshot import SchemaSnapshot


class SchemaDiffer:
    """
    Compares two lists of SchemaSnapshot (previous vs current) and produces
    a list of DriftEvent describing every structural change detected.

    Rules:
    - Object-level events (OBJECT_ADDED, OBJECT_REMOVED) are detected first.
    - Field-level events are only computed for objects present in BOTH snapshots.
    - Field comparison uses SchemaField.is_compatible_with() to distinguish
      TYPE_WIDENED from TYPE_INCOMPATIBLE.
    - Nullability changes are detected independently of type changes.
    """

    def diff(
        self,
        previous: list[SchemaSnapshot],
        current: list[SchemaSnapshot],
    ) -> list[DriftEvent]:
        events: list[DriftEvent] = []
        prev_by_id = {s.object_id: s for s in previous}
        curr_by_id = {s.object_id: s for s in current}

        # Object-level: additions and removals
        for obj_id, curr_snap in curr_by_id.items():
            if obj_id not in prev_by_id:
                events.append(DriftEvent(
                    object_id=obj_id,
                    change_type=DriftChangeType.OBJECT_ADDED,
                    description=f"Object '{curr_snap.object_name}' appeared in source. "
                                f"Registered as unmapped in catalog.",
                ))

        for obj_id, prev_snap in prev_by_id.items():
            if obj_id not in curr_by_id:
                events.append(DriftEvent(
                    object_id=obj_id,
                    change_type=DriftChangeType.OBJECT_REMOVED,
                    description=f"Object '{prev_snap.object_name}' is no longer present "
                                f"in source. Existing pipelines may break.",
                ))

        # Field-level: for objects in both snapshots
        for obj_id in set(prev_by_id) & set(curr_by_id):
            events.extend(self._diff_fields(prev_by_id[obj_id], curr_by_id[obj_id]))

        return events

    def _diff_fields(
        self,
        prev: SchemaSnapshot,
        curr: SchemaSnapshot,
    ) -> list[DriftEvent]:
        events: list[DriftEvent] = []
        prev_fields = {f.name: f for f in prev.fields}
        curr_fields = {f.name: f for f in curr.fields}

        # New fields
        for name, field in curr_fields.items():
            if name not in prev_fields:
                events.append(DriftEvent(
                    object_id=curr.object_id,
                    field_name=name,
                    change_type=DriftChangeType.FIELD_ADDED,
                    description=f"New field '{name}' ({field.normalized_type}) added to source. "
                                f"Added to destination as nullable.",
                    current_value=field.normalized_type,
                ))

        # Removed fields
        for name, field in prev_fields.items():
            if name not in curr_fields:
                events.append(DriftEvent(
                    object_id=curr.object_id,
                    field_name=name,
                    change_type=DriftChangeType.FIELD_REMOVED,
                    description=f"Field '{name}' ({field.normalized_type}) was removed from source. "
                                f"Existing destination field preserved as nullable pending approval.",
                    previous_value=field.normalized_type,
                ))

        # Changed fields
        for name, curr_field in curr_fields.items():
            if name not in prev_fields:
                continue
            prev_field = prev_fields[name]
            events.extend(self._diff_single_field(curr.object_id, prev_field, curr_field))

        return events

    def _diff_single_field(
        self,
        object_id: str,
        prev_field,
        curr_field,
    ) -> list[DriftEvent]:
        events: list[DriftEvent] = []
        name = prev_field.name

        # Type changes
        if prev_field.normalized_type != curr_field.normalized_type:
            if curr_field.is_compatible_with(prev_field):
                events.append(DriftEvent(
                    object_id=object_id,
                    field_name=name,
                    change_type=DriftChangeType.TYPE_WIDENED,
                    description=f"Field '{name}' type widened: "
                                f"{prev_field.normalized_type} → {curr_field.normalized_type}. "
                                f"Schema updated automatically.",
                    previous_value=prev_field.normalized_type,
                    current_value=curr_field.normalized_type,
                ))
            else:
                events.append(DriftEvent(
                    object_id=object_id,
                    field_name=name,
                    change_type=DriftChangeType.TYPE_INCOMPATIBLE,
                    description=f"Field '{name}' type changed incompatibly: "
                                f"{prev_field.normalized_type} → {curr_field.normalized_type}. "
                                f"Requires owner approval before any action.",
                    previous_value=prev_field.normalized_type,
                    current_value=curr_field.normalized_type,
                ))

        # Nullability changes
        if prev_field.nullable and not curr_field.nullable:
            events.append(DriftEvent(
                object_id=object_id,
                field_name=name,
                change_type=DriftChangeType.NULLABLE_TO_REQUIRED,
                description=f"Field '{name}' became required (nullable → non-nullable). "
                            f"Existing NULL values in destination may cause load failures.",
                previous_value="nullable",
                current_value="required",
            ))
        elif not prev_field.nullable and curr_field.nullable:
            events.append(DriftEvent(
                object_id=object_id,
                field_name=name,
                change_type=DriftChangeType.REQUIRED_TO_NULLABLE,
                description=f"Field '{name}' became nullable (required → nullable). "
                            f"Destination schema updated automatically.",
                previous_value="required",
                current_value="nullable",
            ))

        return events
```

- [ ] **Step 2: Testes do SchemaDiffer**

```python
# tests/unit/domain/discovery/services/test_schema_differ.py
from __future__ import annotations

import pytest

from platform.domain.discovery.drift_change_type import DriftChangeType
from platform.domain.discovery.drift_severity import DriftSeverity
from platform.domain.discovery.schema_field import SchemaField
from platform.domain.discovery.schema_snapshot import SchemaSnapshot
from platform.domain.discovery.services.schema_differ import SchemaDiffer


def _snap(object_id: str, fields: list[SchemaField]) -> SchemaSnapshot:
    return SchemaSnapshot(object_id=object_id, object_name=object_id, fields=fields)


def _field(name: str, ntype: str, nullable: bool = True) -> SchemaField:
    return SchemaField(name=name, source_type=ntype.upper(), normalized_type=ntype, nullable=nullable)


def test_no_diff_returns_empty() -> None:
    f = _field("id", "integer")
    snap = _snap("obj-1", [f])
    differ = SchemaDiffer()
    assert differ.diff([snap], [snap]) == []


def test_field_added_is_informative() -> None:
    prev = _snap("obj-1", [_field("id", "integer")])
    curr = _snap("obj-1", [_field("id", "integer"), _field("name", "string")])
    events = SchemaDiffer().diff([prev], [curr])
    assert len(events) == 1
    assert events[0].change_type == DriftChangeType.FIELD_ADDED
    assert events[0].severity == DriftSeverity.INFORMATIVE


def test_field_removed_is_critical() -> None:
    prev = _snap("obj-1", [_field("id", "integer"), _field("name", "string")])
    curr = _snap("obj-1", [_field("id", "integer")])
    events = SchemaDiffer().diff([prev], [curr])
    assert len(events) == 1
    assert events[0].change_type == DriftChangeType.FIELD_REMOVED
    assert events[0].severity == DriftSeverity.CRITICAL


def test_type_widening_is_informative() -> None:
    prev = _snap("obj-1", [_field("id", "integer")])
    curr = _snap("obj-1", [_field("id", "bigint")])
    events = SchemaDiffer().diff([prev], [curr])
    assert events[0].change_type == DriftChangeType.TYPE_WIDENED
    assert events[0].severity == DriftSeverity.INFORMATIVE


def test_type_incompatible_is_critical() -> None:
    prev = _snap("obj-1", [_field("code", "string")])
    curr = _snap("obj-1", [_field("code", "integer")])
    events = SchemaDiffer().diff([prev], [curr])
    assert events[0].change_type == DriftChangeType.TYPE_INCOMPATIBLE
    assert events[0].severity == DriftSeverity.CRITICAL


def test_nullable_to_required_is_critical() -> None:
    prev = _snap("obj-1", [_field("name", "string", nullable=True)])
    curr = _snap("obj-1", [_field("name", "string", nullable=False)])
    events = SchemaDiffer().diff([prev], [curr])
    assert events[0].change_type == DriftChangeType.NULLABLE_TO_REQUIRED
    assert events[0].severity == DriftSeverity.CRITICAL


def test_required_to_nullable_is_informative() -> None:
    prev = _snap("obj-1", [_field("name", "string", nullable=False)])
    curr = _snap("obj-1", [_field("name", "string", nullable=True)])
    events = SchemaDiffer().diff([prev], [curr])
    assert events[0].change_type == DriftChangeType.REQUIRED_TO_NULLABLE
    assert events[0].severity == DriftSeverity.INFORMATIVE


def test_object_added_is_informative() -> None:
    prev: list[SchemaSnapshot] = []
    curr = [_snap("obj-new", [])]
    events = SchemaDiffer().diff(prev, curr)
    assert events[0].change_type == DriftChangeType.OBJECT_ADDED
    assert events[0].severity == DriftSeverity.INFORMATIVE


def test_object_removed_is_critical() -> None:
    prev = [_snap("obj-old", [])]
    curr: list[SchemaSnapshot] = []
    events = SchemaDiffer().diff(prev, curr)
    assert events[0].change_type == DriftChangeType.OBJECT_REMOVED
    assert events[0].severity == DriftSeverity.CRITICAL
```

- [ ] **Step 3: Commit**

```bash
uv run pytest tests/unit/domain/discovery/services/test_schema_differ.py -v
git add platform/domain/discovery/services/schema_differ.py tests/unit/domain/discovery/services/
git commit -m "feat: add SchemaDiffer with complete drift classification matrix (8 change types, informative vs critical)"
```

---

## Task 5: Infrastructure — PolicyTagInferrer + DescriptionGenerator

---

- [ ] **Step 1: Implementar PolicyTagInferrer**

```python
# platform/domain/discovery/services/policy_tag_inferrer.py
from __future__ import annotations

import re

from platform.domain.discovery.policy_tag_confidence import PolicyTagConfidence
from platform.domain.discovery.policy_tag_suggestion import PolicyTagSuggestion
from platform.domain.discovery.schema_field import SchemaField
from platform.domain.shared.policy_tag import PolicyTag


# Inference rules: (pattern, PolicyTag, PolicyTagConfidence)
# Each pattern is a regex matched case-insensitively against the field name.
# More specific patterns come first (highest confidence).
_RULES: list[tuple[str, PolicyTag, PolicyTagConfidence]] = [
    # PII — HIGH confidence (direct name match)
    (r"\bcpf\b", PolicyTag.PII, PolicyTagConfidence.HIGH),
    (r"\bcnpj\b", PolicyTag.PII, PolicyTagConfidence.HIGH),
    (r"\bemail\b", PolicyTag.PII, PolicyTagConfidence.HIGH),
    (r"\bphone\b|\btelefone\b|\bcelular\b", PolicyTag.PII, PolicyTagConfidence.HIGH),
    (r"\bpassport\b|\bpassaporte\b", PolicyTag.PII, PolicyTagConfidence.HIGH),
    (r"\bsocial_security\b|\bssn\b", PolicyTag.PII, PolicyTagConfidence.HIGH),
    (r"\bnome_completo\b|\bfull_name\b", PolicyTag.PII, PolicyTagConfidence.HIGH),
    (r"\bbirth_date\b|\bdata_nasc\b|\bdt_nasc\b", PolicyTag.PII, PolicyTagConfidence.HIGH),

    # RESTRICTED — HIGH (financial/auth secrets)
    (r"\bsenha\b|\bpassword\b|\bpasswd\b", PolicyTag.RESTRICTED, PolicyTagConfidence.HIGH),
    (r"\btoken\b|\baccess_key\b|\bapi_key\b", PolicyTag.RESTRICTED, PolicyTagConfidence.HIGH),
    (r"\bcartao\b|\bcard_number\b|\bcc_num\b", PolicyTag.RESTRICTED, PolicyTagConfidence.HIGH),
    (r"\bcvv\b|\bcvc\b", PolicyTag.RESTRICTED, PolicyTagConfidence.HIGH),
    (r"\bsalario\b|\bsalary\b|\bwage\b|\bpay\b", PolicyTag.RESTRICTED, PolicyTagConfidence.HIGH),

    # PII — MEDIUM (abbreviations or ambiguous names)
    (r"\bdoc\b|\bdocumento\b|\bdoc_num\b", PolicyTag.PII, PolicyTagConfidence.MEDIUM),
    (r"\bbirth\b|\bnascimento\b", PolicyTag.PII, PolicyTagConfidence.MEDIUM),
    (r"\baddress\b|\bendereco\b|\blogradouro\b", PolicyTag.PII, PolicyTagConfidence.MEDIUM),
    (r"\bzip\b|\bcep\b|\bpostal\b", PolicyTag.PII, PolicyTagConfidence.MEDIUM),
    (r"\bgender\b|\bsexo\b|\bgenero\b", PolicyTag.PII, PolicyTagConfidence.MEDIUM),
    (r"\bnacionality\b|\bnacionalidade\b", PolicyTag.PII, PolicyTagConfidence.MEDIUM),

    # RESTRICTED — MEDIUM
    (r"\bsecret\b|\bprivate\b|\bchave\b", PolicyTag.RESTRICTED, PolicyTagConfidence.MEDIUM),
    (r"\baccount\b|\bconta\b", PolicyTag.RESTRICTED, PolicyTagConfidence.MEDIUM),

    # PII — LOW (very general)
    (r"\bname\b|\bnome\b", PolicyTag.PII, PolicyTagConfidence.LOW),
    (r"\bcontato\b|\bcontact\b", PolicyTag.PII, PolicyTagConfidence.LOW),
]

_COMPILED_RULES = [(re.compile(pattern, re.IGNORECASE), tag, confidence)
                   for pattern, tag, confidence in _RULES]


class PolicyTagInferrer:
    """
    Infers PolicyTag suggestions for DataElement fields based on field name patterns.

    Suggestions are NEVER applied automatically — they are returned as
    PolicyTagSuggestion list for owner review. The first matching rule wins
    (most specific → least specific ordering).

    This inferrer uses only field names + normalized types.
    Future versions may incorporate field value sampling for higher accuracy.
    """

    def infer(self, fields: list[SchemaField]) -> list[PolicyTagSuggestion]:
        """
        Infer PolicyTag suggestions for all fields.
        Returns only fields that match at least one rule (no suggestion = no tag inferred).
        """
        suggestions: list[PolicyTagSuggestion] = []
        for field in fields:
            suggestion = self._infer_field(field)
            if suggestion is not None:
                suggestions.append(suggestion)
        return suggestions

    def _infer_field(self, field: SchemaField) -> PolicyTagSuggestion | None:
        for pattern, tag, confidence in _COMPILED_RULES:
            if pattern.search(field.name):
                return PolicyTagSuggestion(
                    field_name=field.name,
                    suggested_tag=tag,
                    confidence=confidence,
                    matched_pattern=pattern.pattern,
                )
        return None
```

- [ ] **Step 2: Implementar DescriptionGenerator**

```python
# platform/domain/discovery/services/description_generator.py
from __future__ import annotations

from platform.domain.discovery.schema_field import SchemaField


class DescriptionGenerator:
    """
    Generates human-readable field descriptions based on field name, type,
    and asset context when the source does not provide native comments.

    Generated descriptions are marked as auto_generated=True in DataElement
    and are always editable by the asset owner.

    This is a rule-based generator (Phase 1). Phase 2 will use an LLM
    to generate context-aware descriptions from the asset business description.
    """

    def generate(self, field: SchemaField, *, asset_name: str, object_name: str) -> str:
        """
        Generate a simple description for a field.
        Example: field 'dt_nasc' in asset 'RH_Colaboradores', object 'colaboradores'
        → "Data relacionada a rh_colaboradores / colaboradores (campo: dt_nasc, tipo: date)."
        """
        readable_name = field.name.replace("_", " ").replace("-", " ")
        return (
            f"{readable_name.capitalize()} field of {object_name} "
            f"in {asset_name} ({field.normalized_type})."
        )

    def generate_all(
        self,
        fields: list[SchemaField],
        *,
        asset_name: str,
        object_name: str,
    ) -> dict[str, str]:
        """Generate descriptions for all fields that lack native descriptions."""
        return {
            field.name: self.generate(field, asset_name=asset_name, object_name=object_name)
            for field in fields
            if not field.description
        }
```

- [ ] **Step 3: Testes**

```python
# tests/unit/domain/discovery/services/test_policy_tag_inferrer.py
from __future__ import annotations

import pytest

from platform.domain.discovery.policy_tag_confidence import PolicyTagConfidence
from platform.domain.discovery.schema_field import SchemaField
from platform.domain.shared.policy_tag import PolicyTag
from platform.domain.discovery.services.policy_tag_inferrer import PolicyTagInferrer


def _field(name: str) -> SchemaField:
    return SchemaField(name=name, source_type="VARCHAR", normalized_type="string")


@pytest.mark.parametrize("field_name,expected_tag,expected_confidence", [
    ("cpf", PolicyTag.PII, PolicyTagConfidence.HIGH),
    ("email", PolicyTag.PII, PolicyTagConfidence.HIGH),
    ("senha", PolicyTag.RESTRICTED, PolicyTagConfidence.HIGH),
    ("cartao", PolicyTag.RESTRICTED, PolicyTagConfidence.HIGH),
    ("birth_date", PolicyTag.PII, PolicyTagConfidence.HIGH),
    ("doc_num", PolicyTag.PII, PolicyTagConfidence.MEDIUM),
    ("name", PolicyTag.PII, PolicyTagConfidence.LOW),
    ("salario", PolicyTag.RESTRICTED, PolicyTagConfidence.HIGH),
])
def test_infer_returns_correct_tag(
    field_name: str, expected_tag: PolicyTag, expected_confidence: PolicyTagConfidence
) -> None:
    inferrer = PolicyTagInferrer()
    suggestions = inferrer.infer([_field(field_name)])
    assert len(suggestions) == 1
    assert suggestions[0].suggested_tag == expected_tag
    assert suggestions[0].confidence == expected_confidence


def test_no_match_returns_empty() -> None:
    inferrer = PolicyTagInferrer()
    suggestions = inferrer.infer([_field("product_sku"), _field("order_total")])
    assert suggestions == []


def test_infer_multiple_fields_only_matches() -> None:
    fields = [_field("cpf"), _field("product_id"), _field("email")]
    suggestions = PolicyTagInferrer().infer(fields)
    assert len(suggestions) == 2
    names = {s.field_name for s in suggestions}
    assert "cpf" in names and "email" in names
```

- [ ] **Step 4: Commit**

```bash
uv run pytest tests/unit/domain/discovery/services/ -v
git add platform/domain/discovery/services/policy_tag_inferrer.py \
        platform/domain/discovery/services/description_generator.py \
        tests/unit/domain/discovery/services/
git commit -m "feat: add PolicyTagInferrer and DescriptionGenerator"
```

---

## Task 6: Infrastructure — DiscoveryRunner Protocol + Runners por Tipo de Endpoint

**Design:** Cada runner implementa o `DiscoveryRunner` Protocol. A factory mapeia `EndpointType → DiscoveryRunner`. Nenhum `if endpoint_type == "..."` fora da factory.

---

- [ ] **Step 1: Criar DiscoveryRunner Protocol**

```python

# platform/infrastructure/discovery/discovery_runner.py
from __future__ import annotations

from typing import Protocol, runtime_checkable

from platform.domain.endpoints.endpoint import Endpoint
from platform.domain.discovery.schema_snapshot import SchemaSnapshot
from platform.domain.assets.data_asset import DataAsset
from platform.domain.shared.secrets.secret_resolver import SecretResolver


@runtime_checkable
class DiscoveryRunner(Protocol):
    """
    Protocol for endpoint-type-specific discovery runners.

    Each runner is responsible for:
    1. Establishing a connection to the endpoint (resolves credentials via the provided SecretResolver).
    2. Applying discovery_scope (include/exclude filters) or target object_id filter.
    3. Returning one SchemaSnapshot per discovered DataObject within scope.

    Runners do NOT persist results — that is the DiscoveryEngine's responsibility.
    Runners MUST be stateless — instantiated per run.
    """

    async def run(
        self,
        endpoint: Endpoint,
        asset: DataAsset,
        secret_resolver: SecretResolver,
        object_id: str | None = None,
    ) -> list[SchemaSnapshot]:
        """
        Connect to endpoint, resolve credentials, filter objects, and return schemas.

        Parameters:
            endpoint: the typed Endpoint (DatabaseEndpoint, RestApiEndpoint, etc.)
            asset: provides discovery_scope (include/exclude patterns)
            secret_resolver: implementation to fetch decrypted credentials safely
            object_id: optional target object name/ID. If provided, the runner
                       MUST bypass the general asset scope and query ONLY this object.

        Returns:
            list of SchemaSnapshot — one per DataObject.

        Raises:
            DiscoveryConnectionError: cannot connect to endpoint
            DiscoveryPermissionError: connected but no read permission
        """
        ...


# platform/infrastructure/discovery/runners/database_runner.py
from __future__ import annotations

import fnmatch
from datetime import datetime, timezone
from sqlalchemy import create_engine, inspect

from platform.domain.assets.data_asset import DataAsset
from platform.domain.discovery.schema_field import SchemaField
from platform.domain.discovery.schema_snapshot import SchemaSnapshot
from platform.domain.endpoints.endpoint import DatabaseEndpoint
from platform.domain.shared.secrets.secret_resolver import SecretResolver
from platform.infrastructure.discovery.discovery_errors import (
    DiscoveryConnectionError,
    DiscoveryPermissionError,
)


# Mapping from driver-specific SQL types to platform normalized ElementType values
_TYPE_MAP: dict[str, str] = {
    "int": "integer", "int4": "integer", "int2": "integer", "integer": "integer",
    "smallint": "integer", "tinyint": "integer",
    "bigint": "bigint", "int8": "bigint",
    "float": "float", "float4": "float", "float8": "float", "real": "float",
    "double precision": "float", "numeric": "decimal", "decimal": "decimal",
    "varchar": "string", "varchar2": "string", "nvarchar": "string",
    "char": "string", "nchar": "string", "text": "string", "clob": "string",
    "date": "date", "timestamp": "timestamp", "timestamptz": "timestamp",
    "datetime": "timestamp", "datetime2": "timestamp",
    "boolean": "boolean", "bool": "boolean", "bit": "boolean",
    "bytea": "bytes", "blob": "bytes", "varbinary": "bytes",
    "json": "json", "jsonb": "json",
}


def _normalize_type(raw_type: str) -> str:
    """Normalize a raw SQL type string to a platform ElementType value."""
    base = raw_type.lower().split("(")[0].strip()
    return _TYPE_MAP.get(base, "string")


class DatabaseRunner:
    """
    DiscoveryRunner for DATABASE endpoints.
    """

    async def run(
        self,
        endpoint: DatabaseEndpoint,
        asset: DataAsset,
        secret_resolver: SecretResolver,
        object_id: str | None = None,
    ) -> list[SchemaSnapshot]:
        try:
            engine = self._create_engine(endpoint, secret_resolver)
        except Exception as exc:
            raise DiscoveryConnectionError(
                f"Cannot connect to database '{endpoint.database}' "
                f"at {endpoint.host}:{endpoint.port}. Error: {exc}"
            ) from exc

        try:
            with engine.connect() as conn:
                insp = inspect(engine)
                table_names = insp.get_table_names() + insp.get_view_names()
        except Exception as exc:
            engine.dispose()
            raise DiscoveryPermissionError(
                f"Connected to database but cannot read schema. "
                f"Check read permissions for credential_ref={endpoint.credential_ref!r}. "
                f"Error: {exc}"
            ) from exc

        # Filter objects: object_id takes precedence over discovery_scope
        if object_id is not None:
            # Extract raw table name if object_id is composite (e.g. asset_id::table_name)
            raw_target = object_id.split("::")[-1]
            filtered = [t for t in table_names if t == raw_target]
        else:
            filtered = self._apply_scope(table_names, asset.discovery_scope)

        snapshots: list[SchemaSnapshot] = []
        now = datetime.now(tz=timezone.utc)

        for table_name in filtered:
            try:
                columns = insp.get_columns(table_name)
                pk_constraint = insp.get_pk_constraint(table_name)
                pk_cols = set(pk_constraint.get("constrained_columns", []))
            except Exception:
                continue

            fields = [
                SchemaField(
                    name=col["name"],
                    source_type=str(col["type"]),
                    normalized_type=_normalize_type(str(col["type"])),
                    nullable=col.get("nullable", True),
                    is_primary_key=col["name"] in pk_cols,
                    description=col.get("comment"),
                )
                for col in columns
            ]
            stable_object_id = f"{asset.id}::{table_name}"
            snapshots.append(SchemaSnapshot(
                object_id=stable_object_id,
                object_name=table_name,
                fields=fields,
                captured_at=now,
                runner_type="database",
            ))

        engine.dispose()
        return snapshots

    def _create_engine(self, endpoint: DatabaseEndpoint, secret_resolver: SecretResolver):
        creds = secret_resolver.resolve(endpoint.credential_ref)
        dsn = (
            f"{endpoint.driver}://{creds['username']}:{creds['password']}"
            f"@{endpoint.host}:{endpoint.port}/{endpoint.database}"
        )
        return create_engine(dsn, pool_pre_ping=True, pool_size=1, max_overflow=0)

    def _apply_scope(self, names: list[str], scope) -> list[str]:
        if scope is None:
            return names
        result = names
        if scope.include:
            result = [n for n in result if any(fnmatch.fnmatch(n, p) for p in scope.include)]
        if scope.exclude:
            result = [n for n in result if not any(fnmatch.fnmatch(n, p) for p in scope.exclude)]
        return result


# platform/infrastructure/discovery/runners/rest_api_runner.py
from __future__ import annotations

from datetime import datetime, timezone
import httpx

from platform.domain.assets.data_asset import DataAsset
from platform.domain.discovery.schema_field import SchemaField
from platform.domain.discovery.schema_snapshot import SchemaSnapshot
from platform.domain.endpoints.endpoint import RestApiEndpoint
from platform.domain.shared.secrets.secret_resolver import SecretResolver
from platform.infrastructure.discovery.discovery_errors import (
    DiscoveryConnectionError,
    DiscoveryPermissionError,
)


class RestApiRunner:
    """
    DiscoveryRunner for REST_API endpoints.
    """

    _OPENAPI_PATHS = ["/openapi.json", "/swagger.json", "/api-docs", "/v3/api-docs", "/v2/api-docs"]

    async def run(
        self,
        endpoint: RestApiEndpoint,
        asset: DataAsset,
        secret_resolver: SecretResolver,
        object_id: str | None = None,
    ) -> list[SchemaSnapshot]:
        creds = secret_resolver.resolve(endpoint.credential_ref)
        headers = self._build_headers(endpoint, creds)

        try:
            client = httpx.Client(base_url=endpoint.base_url, headers=headers, timeout=30.0)
        except Exception as exc:
            raise DiscoveryConnectionError(
                f"Cannot initialize REST client for {endpoint.base_url!r}. Error: {exc}"
            ) from exc

        spec = self._fetch_openapi_spec(client, endpoint.base_url)

        if spec:
            snapshots = self._parse_openapi_spec(spec, asset_id=asset.id)
            if object_id is not None:
                raw_target = object_id.split("::")[-1]
                snapshots = [s for s in snapshots if s.object_name == raw_target]
            return snapshots
        else:
            # Fallback: connectivity-only snapshot
            obj_name = object_id.split("::")[-1] if object_id else "rest_api"
            return [SchemaSnapshot(
                object_id=f"{asset.id}::{obj_name}",
                object_name=obj_name,
                fields=[],
                captured_at=datetime.now(tz=timezone.utc),
                runner_type="rest_api",
            )]

    def _build_headers(self, endpoint: RestApiEndpoint, creds: dict) -> dict[str, str]:
        if endpoint.auth_type == "bearer":
            return {"Authorization": f"Bearer {creds.get('token', '')}"}
        elif endpoint.auth_type == "api_key":
            return {"X-API-Key": creds.get("api_key", "")}
        return {}

    def _fetch_openapi_spec(self, client: httpx.Client, base_url: str) -> dict | None:
        for path in self._OPENAPI_PATHS:
            try:
                resp = client.get(path)
                if resp.status_code == 200:
                    return resp.json()
            except Exception:
                continue
        return None

    def _parse_openapi_spec(self, spec: dict, *, asset_id: str) -> list[SchemaSnapshot]:
        now = datetime.now(tz=timezone.utc)
        snapshots: list[SchemaSnapshot] = []
        schemas = spec.get("components", {}).get("schemas", {}) or \
                  spec.get("definitions", {})

        for schema_name, schema_def in schemas.items():
            fields = self._parse_schema_fields(schema_def)
            snapshots.append(SchemaSnapshot(
                object_id=f"{asset_id}::{schema_name}",
                object_name=schema_name,
                fields=fields,
                captured_at=now,
                runner_type="rest_api",
            ))
        return snapshots

    def _parse_schema_fields(self, schema_def: dict) -> list[SchemaField]:
        _OAS_TYPE_MAP = {
            ("string", None): "string",
            ("string", "date"): "date",
            ("string", "date-time"): "timestamp",
            ("integer", None): "integer",
            ("integer", "int64"): "bigint",
            ("number", None): "float",
            ("number", "double"): "float",
            ("boolean", None): "boolean",
        }
        required_fields = set(schema_def.get("required", []))
        fields: list[SchemaField] = []
        for prop_name, prop_def in schema_def.get("properties", {}).items():
            oas_type = prop_def.get("type", "string")
            oas_fmt = prop_def.get("format")
            normalized = _OAS_TYPE_MAP.get((oas_type, oas_fmt), "string")
            fields.append(SchemaField(
                name=prop_name,
                source_type=f"{oas_type}/{oas_fmt}" if oas_fmt else oas_type,
                normalized_type=normalized,
                nullable=prop_name not in required_fields,
                description=prop_def.get("description"),
            ))
        return fields


# platform/infrastructure/discovery/runners/sftp_runner.py
from __future__ import annotations

import fnmatch
from datetime import datetime, timezone
import paramiko

from platform.domain.assets.data_asset import DataAsset
from platform.domain.discovery.schema_field import SchemaField
from platform.domain.discovery.schema_snapshot import SchemaSnapshot
from platform.domain.endpoints.endpoint import SftpEndpoint
from platform.domain.shared.secrets.secret_resolver import SecretResolver
from platform.infrastructure.discovery.discovery_errors import DiscoveryConnectionError


class SftpRunner:
    """
    DiscoveryRunner for SFTP endpoints.
    """

    async def run(
        self,
        endpoint: SftpEndpoint,
        asset: DataAsset,
        secret_resolver: SecretResolver,
        object_id: str | None = None,
    ) -> list[SchemaSnapshot]:
        try:
            creds = secret_resolver.resolve(endpoint.credential_ref)
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(
                hostname=endpoint.host,
                port=endpoint.port,
                username=creds.get("username"),
                password=creds.get("password"),
                pkey=paramiko.RSAKey.from_private_key_file(creds.get("private_key_path")) if creds.get("private_key_path") else None,
            )
            sftp = ssh.open_sftp()
        except Exception as exc:
            raise DiscoveryConnectionError(
                f"Cannot connect to SFTP {endpoint.host}:{endpoint.port}. Error: {exc}"
            ) from exc

        files = sftp.listdir_attr(endpoint.caminho_raiz)
        now = datetime.now(tz=timezone.utc)
        snapshots: list[SchemaSnapshot] = []

        if object_id is not None:
            raw_target = object_id.split("::")[-1]
            filtered_files = [f for f in files if f.filename == raw_target]
        else:
            scope = asset.discovery_scope
            filtered_files = []
            for attr in files:
                name = attr.filename
                if scope and scope.exclude and any(fnmatch.fnmatch(name, p) for p in scope.exclude):
                    continue
                if scope and scope.include and not any(fnmatch.fnmatch(name, p) for p in scope.include):
                    continue
                filtered_files.append(attr)

        for attr in filtered_files:
            name = attr.filename
            snapshots.append(SchemaSnapshot(
                object_id=f"{asset.id}::{name}",
                object_name=name,
                fields=[
                    SchemaField(name="filename", source_type="varchar", normalized_type="string"),
                    SchemaField(name="size_bytes", source_type="bigint", normalized_type="bigint"),
                    SchemaField(name="last_modified", source_type="timestamp", normalized_type="timestamp"),
                ],
                captured_at=now,
                runner_type="sftp",
                extra={"size": attr.st_size, "mtime": attr.st_mtime},
            ))
        sftp.close()
        ssh.close()
        return snapshots


# platform/infrastructure/discovery/runners/cloud_bucket_runner.py
from __future__ import annotations

import fnmatch
from datetime import datetime, timezone

from platform.domain.assets.data_asset import DataAsset
from platform.domain.discovery.schema_field import SchemaField
from platform.domain.discovery.schema_snapshot import SchemaSnapshot
from platform.domain.endpoints.endpoint import CloudBucketEndpoint
from platform.domain.shared.secrets.secret_resolver import SecretResolver
from platform.infrastructure.discovery.discovery_errors import DiscoveryConnectionError


class CloudBucketRunner:
    """
    DiscoveryRunner for CLOUD_BUCKET endpoints.
    """

    async def run(
        self,
        endpoint: CloudBucketEndpoint,
        asset: DataAsset,
        secret_resolver: SecretResolver,
        object_id: str | None = None,
    ) -> list[SchemaSnapshot]:
        try:
            fs = self._get_filesystem(endpoint, secret_resolver)
            prefix = f"{endpoint.bucket}/{endpoint.prefixo or ''}".rstrip("/")
            all_files = fs.ls(prefix, detail=True)
        except Exception as exc:
            raise DiscoveryConnectionError(
                f"Cannot connect to cloud bucket '{endpoint.bucket}'. Error: {exc}"
            ) from exc

        now = datetime.now(tz=timezone.utc)
        snapshots: list[SchemaSnapshot] = []

        if object_id is not None:
            raw_target = object_id.split("::")[-1]
            filtered_files = [f for f in all_files if f["name"].split("/")[-1] == raw_target]
        else:
            scope = asset.discovery_scope
            filtered_files = []
            for file_info in all_files:
                path = file_info["name"]
                name = path.split("/")[-1]
                if scope and scope.exclude and any(fnmatch.fnmatch(name, p) for p in scope.exclude):
                    continue
                if scope and scope.include and not any(fnmatch.fnmatch(name, p) for p in scope.include):
                    continue
                filtered_files.append(file_info)

        for file_info in filtered_files:
            path = file_info["name"]
            name = path.split("/")[-1]

            fields = self._infer_schema(fs, path, endpoint.provider)
            snapshots.append(SchemaSnapshot(
                object_id=f"{asset.id}::{name}",
                object_name=name,
                fields=fields,
                captured_at=now,
                runner_type="cloud_bucket",
                extra={"path": path, "size": file_info.get("size", 0)},
            ))

        return snapshots

    def _get_filesystem(self, endpoint: CloudBucketEndpoint, secret_resolver: SecretResolver):
        creds = secret_resolver.resolve(endpoint.credential_ref)
        if endpoint.provider == "gcs":
            import gcsfs
            return gcsfs.GCSFileSystem(token=creds.get("service_account_json"))
        elif endpoint.provider == "s3":
            import s3fs
            return s3fs.S3FileSystem(
                key=creds.get("access_key_id"),
                secret=creds.get("secret_access_key"),
                client_kwargs={"region_name": endpoint.regiao},
            )
        else:
            raise NotImplementedError(f"Provider {endpoint.provider!r} not yet supported.")

    def _infer_schema(self, fs, path: str, provider: str) -> list[SchemaField]:
        if path.endswith(".parquet"):
            try:
                import pyarrow.parquet as pq
                pf = pq.ParquetFile(fs.open(path, "rb"))
                schema = pf.schema_arrow
                _PA_MAP = {
                    "int32": "integer", "int64": "bigint", "float32": "float",
                    "float64": "float", "double": "float", "string": "string",
                    "large_string": "string", "utf8": "string", "bool": "boolean",
                    "boolean": "boolean", "timestamp[us]": "timestamp",
                    "date32": "date", "binary": "bytes",
                }
                return [
                    SchemaField(
                        name=field.name,
                        source_type=str(field.type),
                        normalized_type=_PA_MAP.get(str(field.type), "string"),
                        nullable=field.nullable,
                    )
                    for field in schema
                ]
            except Exception:
                return []
        return []


# platform/infrastructure/discovery/runners/etl_flow_runner.py
from __future__ import annotations

from datetime import datetime, timezone
import httpx

from platform.domain.assets.data_asset import DataAsset
from platform.domain.discovery.schema_field import SchemaField
from platform.domain.discovery.schema_snapshot import SchemaSnapshot
from platform.domain.endpoints.endpoint import EtlFlowEndpoint
from platform.domain.shared.secrets.secret_resolver import SecretResolver
from platform.infrastructure.discovery.discovery_errors import DiscoveryConnectionError


class EtlFlowRunner:
    """
    DiscoveryRunner for ETL_FLOW endpoints.
    """

    async def run(
        self,
        endpoint: EtlFlowEndpoint,
        asset: DataAsset,
        secret_resolver: SecretResolver,
        object_id: str | None = None,
    ) -> list[SchemaSnapshot]:
        try:
            snapshots = self._run_fivetran(endpoint, asset, secret_resolver)
            if object_id is not None:
                raw_target = object_id.split("::")[-1]
                snapshots = [s for s in snapshots if s.object_name == raw_target]
            return snapshots
        except Exception as exc:
            raise DiscoveryConnectionError(
                f"Cannot connect to ETL flow {endpoint.ferramenta!r} "
                f"connector {endpoint.flow_id!r}. Error: {exc}"
            ) from exc

    def _run_fivetran(
        self,
        endpoint: EtlFlowEndpoint,
        asset: DataAsset,
        secret_resolver: SecretResolver,
    ) -> list[SchemaSnapshot]:
        creds = secret_resolver.resolve(endpoint.credential_ref)
        api_key = creds.get("api_key")
        api_secret = creds.get("api_secret")
        now = datetime.now(tz=timezone.utc)
        client = httpx.Client(
            base_url="https://api.fivetran.com/v1",
            auth=(api_key, api_secret),
            timeout=30.0,
        )
        resp = client.get(f"/connectors/{endpoint.flow_id}/schemas")
        resp.raise_for_status()
        schemas_data = resp.json().get("data", {}).get("schemas", {})
        snapshots: list[SchemaSnapshot] = []
        for schema_name, schema_def in schemas_data.items():
            for table_name, table_def in schema_def.get("tables", {}).items():
                fields = [
                    SchemaField(
                        name=col_name,
                        source_type=col_def.get("data_type", "string"),
                        normalized_type=col_def.get("data_type", "string"),
                        nullable=True,
                        is_primary_key=col_def.get("is_primary_key", False),
                    )
                    for col_name, col_def in table_def.get("columns", {}).items()
                ]
                snapshots.append(SchemaSnapshot(
                    object_id=f"{asset.id}::{schema_name}.{table_name}",
                    object_name=f"{schema_name}.{table_name}",
                    fields=fields,
                    captured_at=now,
                    runner_type="etl_flow",
                ))
        return snapshots


# platform/application/discovery/service/discovery_engine.py
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from platform.domain.assets.data_asset import DataAsset
from platform.domain.discovery.discovery_run import DiscoveryRun
from platform.domain.discovery.discovery_run_repository import DiscoveryRunRepository
from platform.domain.discovery.drift_approval import DriftApproval, DriftApprovalDecision
from platform.domain.discovery.drift_approval_repository import DriftApprovalRepository
from platform.domain.endpoints.endpoint import Endpoint
from platform.domain.endpoints.endpoint_type import EndpointType
from platform.domain.shared.secrets.secret_resolver import SecretResolver
from platform.domain.discovery.services.description_generator import DescriptionGenerator
from platform.infrastructure.discovery.discovery_errors import (
    DiscoveryConnectionError,
    DiscoveryPermissionError,
)
from platform.infrastructure.discovery.discovery_runner_factory import get_discovery_runner
from platform.domain.discovery.services.policy_tag_inferrer import PolicyTagInferrer
from platform.domain.discovery.services.schema_differ import SchemaDiffer


class DiscoveryEngine:
    """
    Orchestrates a full Discovery execution for a DataAsset.

    Responsibilities:
    1. Resolve the correct runner from the endpoint type.
    2. Run the runner to collect SchemaSnapshot list (optionally for a target object_id).
    3. Load the previous SchemaSnapshot list from the last completed DiscoveryRun.
    4. Diff previous vs current to produce DriftEvent list.
    5. Infer PolicyTag suggestions (soft).
    6. Generate auto descriptions (soft).
    7. Persist the DiscoveryRun aggregate.
    8. Create DriftApproval records for every CRITICAL DriftEvent.
    9. Return the persisted DiscoveryRun.
    """

    def __init__(
        self,
        discovery_run_repo: DiscoveryRunRepository,
        drift_approval_repo: DriftApprovalRepository,
        secret_resolver: SecretResolver,
    ) -> None:
        self._run_repo = discovery_run_repo
        self._approval_repo = drift_approval_repo
        self._secret_resolver = secret_resolver
        self._differ = SchemaDiffer()
        self._inferrer = PolicyTagInferrer()
        self._desc_generator = DescriptionGenerator()

    async def execute(
        self,
        *,
        asset: DataAsset,
        endpoint: Endpoint,
        triggered_by: str = "schedule",
        object_id: str | None = None,
    ) -> DiscoveryRun:
        """
        Execute a full Discovery run for the given asset and endpoint.
        Optionally filter scope to a single object_id (for inline pipeline validation).
        """
        run = DiscoveryRun(
            id=str(uuid.uuid4()),
            asset_id=asset.id,
            triggered_by=triggered_by,
        )
        run = await self._run_repo.save(run)

        run.start()
        run = await self._run_repo.save(run)

        try:
            # Step 1: Collect current schema (optionally pass target object_id)
            runner = get_discovery_runner(EndpointType(endpoint.tipo))
            # Calls runner to discover structure (async to support many objects without blocking)
            current_snapshots = await runner.run(
                endpoint=endpoint,
                asset=asset,
                secret_resolver=self._secret_resolver,
                object_id=object_id,
            )

            # Step 2: Load previous snapshot baseline (limit to object_id if running inline)
            previous_run = await self._run_repo.find_latest_by_asset_id(asset.id)
            if previous_run:
                previous_snapshots = previous_run.snapshots
                if object_id is not None:
                    previous_snapshots = [s for s in previous_snapshots if s.object_id == object_id]
            else:
                previous_snapshots = []

            # Step 3: Diff
            drift_events = self._differ.diff(previous_snapshots, current_snapshots)

            # Step 4: PolicyTag inference
            policy_suggestions = []
            soft_failures: list[str] = []
            try:
                all_fields = [f for snap in current_snapshots for f in snap.fields]
                policy_suggestions = self._inferrer.infer(all_fields)
            except Exception as exc:
                soft_failures.append(f"PolicyTagInferrer: {exc}")

            # Step 5: Description generation
            auto_descriptions: dict[str, str] = {}
            try:
                for snap in current_snapshots:
                    auto_descriptions.update(
                        self._desc_generator.generate_all(
                            snap.fields,
                            asset_name=asset.nome,
                            object_name=snap.object_name,
                        )
                    )
            except Exception as exc:
                soft_failures.append(f"DescriptionGenerator: {exc}")

            # Step 6: Complete the run
            run.complete(
                snapshots=current_snapshots,
                drift_events=drift_events,
                policy_tag_suggestions=policy_suggestions,
                auto_generated_descriptions=auto_descriptions,
                soft_failures=soft_failures,
            )
            run = await self._run_repo.save(run)

            # Step 7: Create DriftApproval for each CRITICAL event
            for event in run.critical_events:
                approval = DriftApproval(
                    id=str(uuid.uuid4()),
                    discovery_run_id=run.id,
                    asset_id=asset.id,
                    object_id=event.object_id,
                    field_name=event.field_name,
                    change_type=event.change_type,
                    severity_description=event.description,
                )
                await self._approval_repo.save(approval)

        except (DiscoveryConnectionError, DiscoveryPermissionError) as exc:
            run.fail(error_message=str(exc))
            run = await self._run_repo.save(run)

        return run

```

- [ ] **Step 2: Testes do DiscoveryEngine com FakeRunner**

```python
# tests/unit/infrastructure/discovery/test_discovery_engine.py
from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from platform.domain.discovery.discovery_run import DiscoveryRun
from platform.domain.discovery.discovery_run_status import DiscoveryRunStatus
from platform.domain.discovery.drift_change_type import DriftChangeType
from platform.domain.discovery.schema_field import SchemaField
from platform.domain.discovery.schema_snapshot import SchemaSnapshot
from platform.application.discovery.service.discovery_engine import DiscoveryEngine
from platform.infrastructure.discovery.discovery_errors import DiscoveryConnectionError


def _asset(id_: str = "asset-1", nome: str = "Test Asset") -> Any:
    """Minimal DataAsset fake."""
    asset = MagicMock()
    asset.id = id_
    asset.nome = nome
    asset.discovery_scope = None
    return asset


def _endpoint(tipo: str = "database") -> Any:
    endpoint = MagicMock()
    endpoint.tipo = tipo
    return endpoint


def _snapshot(object_id: str, fields: list[SchemaField] | None = None) -> SchemaSnapshot:
    return SchemaSnapshot(object_id=object_id, object_name=object_id, fields=fields or [])


class FakeDiscoveryRunRepo:
    def __init__(self, *, previous_run: DiscoveryRun | None = None):
        self._runs: dict[str, DiscoveryRun] = {}
        self._previous = previous_run

    async def save(self, run: DiscoveryRun) -> DiscoveryRun:
        self._runs[run.id] = run
        return run

    async def find_by_id(self, run_id: str) -> DiscoveryRun | None:
        return self._runs.get(run_id)

    async def find_latest_by_asset_id(self, asset_id: str) -> DiscoveryRun | None:
        return self._previous

    async def find_all_by_asset_id(self, asset_id: str, *, limit: int = 20) -> list[DiscoveryRun]:
        return list(self._runs.values())


class FakeDriftApprovalRepo:
    def __init__(self):
        self.saved: list[Any] = []

    async def save(self, approval: Any) -> Any:
        self.saved.append(approval)
        return approval

    async def find_by_id(self, approval_id: str) -> Any:
        return None

    async def find_pending_by_asset_id(self, asset_id: str) -> list[Any]:
        return self.saved

    async def find_by_discovery_run_id(self, discovery_run_id: str) -> list[Any]:
        return [a for a in self.saved if a.discovery_run_id == discovery_run_id]


@pytest.mark.asyncio
async def test_engine_completes_run_on_success() -> None:
    """Engine produces a COMPLETED run when runner succeeds."""
    run_repo = FakeDiscoveryRunRepo()
    approval_repo = FakeDriftApprovalRepo()
    engine = DiscoveryEngine(discovery_run_repo=run_repo, drift_approval_repo=approval_repo)

    # Patch the runner factory to return a fake runner
    fake_runner = AsyncMock()
    fake_runner.run.return_value = [_snapshot("obj-1")]

    import platform.application.discovery.service.discovery_engine as eng_module
    original_factory = eng_module.get_discovery_runner
    eng_module.get_discovery_runner = lambda _: fake_runner

    try:
        run = await engine.execute(asset=_asset(), endpoint=_endpoint())
    finally:
        eng_module.get_discovery_runner = original_factory

    assert run.status == DiscoveryRunStatus.COMPLETED
    assert run.objects_discovered == 1


@pytest.mark.asyncio
async def test_engine_records_failure_on_connection_error() -> None:
    """Engine produces a FAILED run when runner raises DiscoveryConnectionError."""
    run_repo = FakeDiscoveryRunRepo()
    approval_repo = FakeDriftApprovalRepo()
    engine = DiscoveryEngine(discovery_run_repo=run_repo, drift_approval_repo=approval_repo)

    failing_runner = AsyncMock()
    failing_runner.run.side_effect = DiscoveryConnectionError("refused")

    import platform.application.discovery.service.discovery_engine as eng_module
    original_factory = eng_module.get_discovery_runner
    eng_module.get_discovery_runner = lambda _: failing_runner

    try:
        run = await engine.execute(asset=_asset(), endpoint=_endpoint())
    finally:
        eng_module.get_discovery_runner = original_factory

    assert run.status == DiscoveryRunStatus.FAILED
    assert "refused" in run.error_message


@pytest.mark.asyncio
async def test_engine_creates_drift_approvals_for_critical_events() -> None:
    """Engine creates one DriftApproval per CRITICAL drift event."""
    # Previous snapshot: field "cpf" is STRING
    prev_snap = SchemaSnapshot(
        object_id="obj-1", object_name="customers",
        fields=[SchemaField(name="cpf", source_type="VARCHAR", normalized_type="string")],
    )
    prev_run = DiscoveryRun(id=str(uuid.uuid4()), asset_id="asset-1", triggered_by="schedule",
                             status=DiscoveryRunStatus.COMPLETED)
    prev_run.snapshots = [prev_snap]

    run_repo = FakeDiscoveryRunRepo(previous_run=prev_run)
    approval_repo = FakeDriftApprovalRepo()
    engine = DiscoveryEngine(discovery_run_repo=run_repo, drift_approval_repo=approval_repo)

    # Current snapshot: field "cpf" is INTEGER (incompatible change)
    fake_runner = AsyncMock()
    fake_runner.run.return_value = [SchemaSnapshot(
        object_id="obj-1", object_name="customers",
        fields=[SchemaField(name="cpf", source_type="INT", normalized_type="integer")],
    )]

    import platform.application.discovery.service.discovery_engine as eng_module
    original_factory = eng_module.get_discovery_runner
    eng_module.get_discovery_runner = lambda _: fake_runner

    try:
        run = await engine.execute(asset=_asset(), endpoint=_endpoint())
    finally:
        eng_module.get_discovery_runner = original_factory

    assert run.has_critical_drift is True
    assert len(approval_repo.saved) == 1
    assert approval_repo.saved[0].change_type == DriftChangeType.TYPE_INCOMPATIBLE
```

- [ ] **Step 3: Commit**

```bash
uv run pytest tests/unit/infrastructure/discovery/test_discovery_engine.py -v
git add platform/application/discovery/service/discovery_engine.py \
        tests/unit/infrastructure/discovery/test_discovery_engine.py
git commit -m "feat: add DiscoveryEngine orchestrator (runner + differ + inferrer + DriftApproval generation)"
```

---

## Task 8: Application — Use Cases

---

- [ ] **Step 1: RunDiscoveryUseCase**

```python
# platform/application/discovery/platform_client.py
from __future__ import annotations

from typing import Protocol


class PlatformClient(Protocol):
    """
    Protocol/Interface defining the core platform callback actions.
    Decouples the application layer (use cases) from the concrete HTTP client implementation.
    """

    def apply_drift_self_healing(
        self,
        approval_id: str,
        asset_id: str,
        object_id: str,
        field_name: str | None,
        change_type: str,
    ) -> None:
        """Apply structural updates in the target system on owner approval."""
        ...

    def pause_pipelines_for_object(
        self,
        asset_id: str,
        object_id: str,
        reason: str,
    ) -> None:
        """Pause execution of all pipelines depending on the target object."""
        ...


# platform/application/discovery/run_discovery.py
from __future__ import annotations

import uuid

from platform.application.unit_of_work import UnitOfWork
from platform.domain.assets.asset_repository import AssetRepository
from platform.domain.endpoints.endpoint_repository import EndpointRepository
from platform.application.discovery.service.discovery_engine import DiscoveryEngine


class DiscoveryAssetNotFoundError(Exception):
    pass


class DiscoveryEndpointNotFoundError(Exception):
    pass


class RunDiscoveryUseCase:
    """
    Application use case: trigger a Discovery run for a DataAsset.

    Optionally accepts object_id to execute inline object-level validation.
    """

    def __init__(
        self,
        uow: UnitOfWork,
        engine: DiscoveryEngine,
    ) -> None:
        self._uow = uow
        self._engine = engine

    async def execute(
        self,
        *,
        asset_id: str,
        triggered_by: str = "manual",
        object_id: str | None = None,
    ) -> str:
        """
        Trigger discovery for the given asset_id, optionally scoped to object_id.
        """
        async with self._uow:
            asset = await self._uow.assets.find_by_id(asset_id)
            if not asset:
                raise DiscoveryAssetNotFoundError(f"Asset not found: {asset_id!r}")

            endpoint = await self._uow.endpoints.find_by_id(asset.endpoint_id)
            if not endpoint:
                raise DiscoveryEndpointNotFoundError(
                    f"Endpoint not found for asset {asset_id!r}: "
                    f"endpoint_id={asset.endpoint_id!r}"
                )

        run = await self._engine.execute(
            asset=asset,
            endpoint=endpoint,
            triggered_by=triggered_by,
            object_id=object_id,
        )
        return run.id
```

- [ ] **Step 2: ApproveDriftUseCase e RejectDriftUseCase**

```python
# platform/application/discovery/approve_drift.py
from __future__ import annotations

from platform.application.unit_of_work import UnitOfWork
from platform.application.discovery.platform_client import PlatformClient


class DriftApprovalNotFoundError(Exception):
    pass


class DriftApprovalAlreadyDecidedError(Exception):
    pass


class ApproveDriftUseCase:
    """
    Application use case: owner approves a critical drift.
    Uses the injected PlatformClient interface (no direct infrastructure coupling).
    """

    def __init__(self, uow: UnitOfWork, platform_client: PlatformClient) -> None:
        self._uow = uow
        self._platform_client = platform_client

    async def execute(
        self,
        *,
        approval_id: str,
        decided_by: str,
        notes: str | None = None,
    ) -> None:
        async with self._uow:
            approval = await self._uow.drift_approvals.find_by_id(approval_id)
            if not approval:
                raise DriftApprovalNotFoundError(approval_id)
            if not approval.is_pending:
                raise DriftApprovalAlreadyDecidedError(
                    f"DriftApproval {approval_id!r} already decided: {approval.decision!r}"
                )
            approval.approve(decided_by=decided_by, notes=notes)
            await self._uow.drift_approvals.save(approval)
            await self._uow.commit()

        self._platform_client.apply_drift_self_healing(
            approval_id=approval_id,
            asset_id=approval.asset_id,
            object_id=approval.object_id,
            field_name=approval.field_name,
            change_type=approval.change_type.value,
        )
```

```python
# platform/application/discovery/reject_drift.py
from __future__ import annotations

from platform.application.unit_of_work import UnitOfWork
from platform.application.discovery.platform_client import PlatformClient


class RejectDriftUseCase:
    """
    Application use case: owner rejects a critical drift.
    Uses the injected PlatformClient interface.
    """

    def __init__(self, uow: UnitOfWork, platform_client: PlatformClient) -> None:
        self._uow = uow
        self._platform_client = platform_client

    async def execute(
        self,
        *,
        approval_id: str,
        decided_by: str,
        notes: str | None = None,
    ) -> None:
        async with self._uow:
            approval = await self._uow.drift_approvals.find_by_id(approval_id)
            if not approval:
                from platform.application.discovery.approve_drift import DriftApprovalNotFoundError
                raise DriftApprovalNotFoundError(approval_id)
            if not approval.is_pending:
                from platform.application.discovery.approve_drift import DriftApprovalAlreadyDecidedError
                raise DriftApprovalAlreadyDecidedError(
                    f"DriftApproval {approval_id!r} already decided: {approval.decision!r}"
                )
            approval.reject(decided_by=decided_by, notes=notes)
            await self._uow.drift_approvals.save(approval)
            await self._uow.commit()

        self._platform_client.pause_pipelines_for_object(
            asset_id=approval.asset_id,
            object_id=approval.object_id,
            reason=f"Critical schema drift rejected by {decided_by}. "
                   f"change_type={approval.change_type.value!r}. "
                   f"Manual intervention required.",
        )
```

- [ ] **Step 3: Commit**

```bash
git add platform/application/discovery/
git commit -m "feat: add RunDiscovery, ApproveDrift, RejectDrift use cases"
```

---

## Task 9: HTTP — Routers e Schemas

---

- [ ] **Step 1: Schemas Pydantic**

```python
# platform/infrastructure/http/schemas/discovery_schemas.py
from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class TriggerDiscoveryRequest(BaseModel):
    asset_id: str
    triggered_by: Literal["manual", "schedule", "initial"] = "manual"
    object_id: str | None = Field(default=None, description="Optional target object name to run discovery exclusively for (inline mode).")


class TriggerDiscoveryResponse(BaseModel):
    discovery_run_id: str
    message: str = "Discovery triggered. Poll /discovery/runs/{id} for status."



class DriftEventSchema(BaseModel):
    object_id: str
    field_name: str | None = None
    change_type: str
    severity: str
    description: str
    previous_value: str | None = None
    current_value: str | None = None
    requires_approval: bool


class PolicyTagSuggestionSchema(BaseModel):
    field_name: str
    suggested_tag: str
    confidence: str
    matched_pattern: str
    auto_generated_description: str | None = None


class DiscoveryRunSchema(BaseModel):
    id: str
    asset_id: str
    triggered_by: str
    status: str
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error_message: str | None = None
    objects_discovered: int
    fields_discovered: int
    drift_events: list[DriftEventSchema] = Field(default_factory=list)
    policy_tag_suggestions: list[PolicyTagSuggestionSchema] = Field(default_factory=list)
    soft_failures: list[str] = Field(default_factory=list)
    duration_seconds: float | None = None
    has_critical_drift: bool
    created_at: datetime
    updated_at: datetime


class DriftApprovalSchema(BaseModel):
    id: str
    discovery_run_id: str
    asset_id: str
    object_id: str
    field_name: str | None = None
    change_type: str
    severity_description: str
    decision: str
    decided_by: str | None = None
    decided_at: datetime | None = None
    owner_notes: str | None = None


class DecideDriftRequest(BaseModel):
    notes: str | None = None
```

- [ ] **Step 2: Router**

```python
# platform/infrastructure/http/routers/discovery_router.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from platform.application.discovery.approve_drift import (
    ApproveDriftUseCase,
    DriftApprovalAlreadyDecidedError,
    DriftApprovalNotFoundError,
)
from platform.application.discovery.reject_drift import RejectDriftUseCase
from platform.application.discovery.run_discovery import (
    DiscoveryAssetNotFoundError,
    DiscoveryEndpointNotFoundError,
    RunDiscoveryUseCase,
)
from platform.auth.dependencies import require_role
from platform.auth.role import Role
from platform.infrastructure.http.schemas.discovery_schemas import (
    DecideDriftRequest,
    DriftApprovalSchema,
    DiscoveryRunSchema,
    TriggerDiscoveryRequest,
    TriggerDiscoveryResponse,
)

router = APIRouter(prefix="/discovery", tags=["Discovery"])


@router.post(
    "/trigger",
    response_model=TriggerDiscoveryResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Trigger a Discovery run for a DataAsset",
)
async def trigger_discovery(
    body: TriggerDiscoveryRequest,
    current_user=Depends(require_role([Role.ANALYTICS_ENGINEER, Role.PRODUCT_OWNER])),
    use_case: RunDiscoveryUseCase = Depends(),
) -> TriggerDiscoveryResponse:
    """
    Trigger a Discovery run for the given asset_id, optionally scoped to a single object_id.
    Returns the run ID for async status polling.
    """
    try:
        run_id = await use_case.execute(
            asset_id=body.asset_id,
            triggered_by=body.triggered_by,
            object_id=body.object_id,
        )
    except DiscoveryAssetNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except DiscoveryEndpointNotFoundError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return TriggerDiscoveryResponse(discovery_run_id=run_id)



@router.get(
    "/runs/{run_id}",
    response_model=DiscoveryRunSchema,
    summary="Get Discovery run status and results",
)
async def get_discovery_run(
    run_id: str,
    current_user=Depends(require_role([Role.ANALYTICS_ENGINEER, Role.PRODUCT_OWNER, Role.SRE])),
    # run_repo injected via dependency
) -> DiscoveryRunSchema:
    """Return full result of a Discovery run including drift events and PolicyTag suggestions."""
    # Implementation: inject SqlDiscoveryRunRepository and call find_by_id
    raise HTTPException(status_code=501, detail="Not implemented yet")


@router.get(
    "/assets/{asset_id}/runs",
    response_model=list[DiscoveryRunSchema],
    summary="List Discovery runs for a DataAsset",
)
async def list_asset_runs(
    asset_id: str,
    current_user=Depends(require_role([Role.ANALYTICS_ENGINEER, Role.PRODUCT_OWNER])),
) -> list[DiscoveryRunSchema]:
    """Return the last 20 Discovery runs for an asset (most recent first)."""
    raise HTTPException(status_code=501, detail="Not implemented yet")


@router.get(
    "/assets/{asset_id}/approvals",
    response_model=list[DriftApprovalSchema],
    summary="List pending drift approvals for an asset",
)
async def list_pending_approvals(
    asset_id: str,
    current_user=Depends(require_role([Role.ANALYTICS_ENGINEER, Role.PRODUCT_OWNER])),
) -> list[DriftApprovalSchema]:
    """Return all PENDING DriftApprovals for an asset — for the owner to act on."""
    raise HTTPException(status_code=501, detail="Not implemented yet")


@router.post(
    "/approvals/{approval_id}/approve",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Approve a critical drift change",
)
async def approve_drift(
    approval_id: str,
    body: DecideDriftRequest,
    current_user=Depends(require_role([Role.ANALYTICS_ENGINEER, Role.PRODUCT_OWNER])),
    use_case: ApproveDriftUseCase = Depends(),
) -> None:
    """Owner approves the critical schema change. Platform applies self-healing action."""
    try:
        await use_case.execute(
            approval_id=approval_id,
            decided_by=current_user.id,
            notes=body.notes,
        )
    except DriftApprovalNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except DriftApprovalAlreadyDecidedError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.post(
    "/approvals/{approval_id}/reject",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Reject a critical drift change",
)
async def reject_drift(
    approval_id: str,
    body: DecideDriftRequest,
    current_user=Depends(require_role([Role.ANALYTICS_ENGINEER, Role.PRODUCT_OWNER])),
    use_case: RejectDriftUseCase = Depends(),
) -> None:
    """Owner rejects the critical schema change. Affected pipelines are paused."""
    try:
        await use_case.execute(
            approval_id=approval_id,
            decided_by=current_user.id,
            notes=body.notes,
        )
    except DriftApprovalNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except DriftApprovalAlreadyDecidedError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
```

- [ ] **Step 3: Commit**

```bash
git add platform/infrastructure/http/schemas/discovery_schemas.py \
        platform/infrastructure/http/routers/discovery_router.py
git commit -m "feat: add Discovery HTTP router + Pydantic schemas (trigger, run status, approvals)"
```

---

## Task 10: Testes Completos + CI Gate

---

- [ ] **Step 1: Testes unitários completos**

```bash
uv run pytest tests/unit/domain/discovery/ -v
uv run pytest tests/unit/infrastructure/discovery/ -v
```

Esperado: todos passam.

- [ ] **Step 2: Testes de contrato HTTP (esqueleto)**

```python
# tests/contract/test_discovery_contract.py
"""
Contract tests for Discovery API.
Verifies schema shape of response bodies — not business logic.
"""
from __future__ import annotations

# POST /discovery/trigger → 202 + {"discovery_run_id": str, "message": str}
# GET /discovery/runs/{id} → 200 + DiscoveryRunSchema
# GET /discovery/assets/{id}/approvals → 200 + list[DriftApprovalSchema]
# POST /discovery/approvals/{id}/approve → 204
# POST /discovery/approvals/{id}/reject → 204
```

- [ ] **Step 3: Rodar CI gate**

```bash
make check
```

Esperado: `✅ All checks passed.`

- [ ] **Step 4: Commit final**

```bash
git add .
git commit -m "feat: plan-03 complete — Discovery Engine (5 typed runners, SchemaDiffer, PolicyTagInferrer, DiscoveryRun aggregate, DriftApproval workflow, RunDiscovery/ApproveDrift/RejectDrift use cases)"
```

---

## Self-Review

| Item | Decisão |
|---|---|
| **Polimorfismo de runners** | `DiscoveryRunner` Protocol. 5 runners (`database`, `rest_api`, `sftp`, `cloud_bucket`, `etl_flow`). Factory dict — sem if/elif. Novo tipo: adicionar runner + uma linha no dict. |
| **Object-level Ingestion / Inline Validation** | Suporte a `object_id` opcional em toda a cadeia de execução (`RunDiscoveryUseCase`, `DiscoveryEngine`, `DiscoveryRunner`). Permite validação inline leve em pipelines limitando a varredura à tabela/arquivo de interesse, enquanto a execução via schedule do Asset (`discovery_schedule`) faz a descoberta completa. |
| **Clean Architecture & UoW** | Use cases usam o `PlatformClient` (Protocol) injetado ao invés de depender diretamente de `platform.infrastructure.platform_client.get_platform_client`. |
| **Desacoplamento de Segredos** | `SecretResolver` (Protocol) definido no domínio e injetado para resolver credenciais sem dependência física do Vault em testes unitários. |
| **SchemaDiffer** | Puro Python. Nenhuma dependência externa. 8 `DriftChangeType`. Classificação determinística via `_SEVERITY_MAP` em `DriftEvent.severity`. |
| **PolicyTagInferrer** | 30+ regras regex por prioridade (HIGH → MEDIUM → LOW). Retorna `PolicyTagSuggestion` — nunca aplica diretamente. Owner confirma. |
| **DiscoveryRun aggregate** | `PENDING → RUNNING → COMPLETED / FAILED / PARTIAL`. JSON serialization das snapshots no ORM. |
| **DriftApproval** | Uma por DriftEvent CRITICAL. `PENDING → APPROVED / REJECTED`. `ApproveDriftUseCase` aplica self-healing. `RejectDriftUseCase` pausa pipelines afetados. |
| **Optional steps** | PolicyTagInferrer e DescriptionGenerator são soft — falhas capturadas em `soft_failures`, run transita para `PARTIAL`. |
| **Auditoria** | `DiscoveryRun` persiste em `PENDING` antes de iniciar — visível no dashboard antes mesmo do runner conectar. |
| **Aprovação de mudanças críticas** | Nunca auto-aprovadas. `DriftApproval` em `PENDING` bloqueia self-healing até decisão explícita do owner. |

**Próximo plano:** `2026-07-XX-plan-04-catalog-integration.md` — Integração com DataHub/OpenMetadata (catalog adapter), endpoint de linhagem end-to-end, RBAC granular por DataAsset, e CLI completa com `platform discovery run` e `platform drift approve`.

