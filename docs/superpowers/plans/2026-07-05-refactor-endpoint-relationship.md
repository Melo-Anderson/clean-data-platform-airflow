# Refactor Endpoint-DataAsset Relationship Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor Endpoint-DataAsset relationship to remove `asset_id` from Endpoint, add `name` to Endpoint, invert flow to make Endpoints independent, change API routes and payloads to use names instead of IDs, add a DataAsset update API, and maintain full test correctness.

**Architecture:**
- Clean Architecture boundaries are maintained.
- Domain layer models (`Endpoint`, `DataAsset`) remain decoupled from frameworks.
- Infrastructure layer models (`EndpointModel`) drop `asset_id` and gain a unique `name`.
- API controllers (Routers) receive user-friendly names (`asset_name`, `endpoint_name`) and resolve them to internal system IDs before calling the Domain layer.
- Updates to DataAssets are published to the catalog and trigger alerts, matching registration behavior.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy 2.0, Pydantic 2.x, Pytest-asyncio.

---

## Global Constraints

- No new external dependencies.
- Follow TDD pattern: write failing tests first, run to verify failure, implement code, verify passing, and commit.
- Update all existing unit and integration tests that reference `asset_id` on endpoints, or lookup endpoints by asset ID.
- Preserve audit fields and all existing database columns not related to this refactoring.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `app/domain/endpoints/endpoint.py` | **Modify** | Remove `asset_id` and add unique `name` to Endpoint entity |
| `app/infrastructure/persistence/models/endpoint_model.py` | **Modify** | Update database schema columns (drop `asset_id`, add unique `name`) |
| `app/domain/endpoints/endpoint_repository.py` | **Modify** | Update repo contract interface (add `find_by_name`, remove `find_by_asset_id`) |
| `app/infrastructure/persistence/repositories/sql_endpoint_repository.py` | **Modify** | Remove `find_by_asset_id` method, add `find_by_name`, update `_to_domain`/`_to_model` |
| `app/domain/assets/asset_repository.py` | **Modify** | Add `find_by_name` and `update` to repository interface |
| `app/infrastructure/persistence/repositories/sql_asset_repository.py` | **Modify** | Implement `find_by_name` and `update` in SQL asset repository |
| `app/domain/assets/asset_service.py` | **Modify** | Add generic `update` domain logic |
| `app/application/endpoints/provision_endpoint.py` | **Modify** | Update use case to take `name` instead of `asset_id` |
| `app/infrastructure/http/routers/endpoint_router.py` | **Modify** | Update endpoint routing payloads to use `name` |
| `app/infrastructure/http/schemas/asset_schemas.py` | **Modify** | Add `AssetUpdateRequest` schema; `AssetResponse` keeps `endpoint_id` (input is name, output is ID) |
| `app/application/assets/activate_asset.py` | **Modify** | Use case receives `asset_id` and `endpoint_id` (IDs only — name resolution moved to Router) |
| `app/application/assets/update_asset.py` | **Create** | Use case receives `asset_id` and optional kwargs with IDs (name resolution in Router) |
| `app/infrastructure/http/routers/asset_router.py` | **Modify** | Router resolves `asset_name`→ID and `endpoint_name`→ID before calling Use Cases; adds `PUT` update route |
| `app/infrastructure/http/routers/discovery_router.py` | **Modify** | Router resolves `asset_name`→ID before calling `RunDiscoveryUseCase` |
| `tests/unit/domain/endpoints/test_endpoint_service.py` | **Modify** | Update fake repo and tests to adapt to `name` and remove `asset_id` |
| `tests/unit/application/test_activate_asset.py` | **Modify** | Fix tests — use case now receives `asset_id` and `endpoint_id` directly |
| `tests/unit/application/test_update_asset.py` | **Create** | Unit tests for `UpdateAssetUseCase` with ID-based interface |
| `tests/unit/application/test_unit_of_work.py` | **Modify** | Remove `find_by_asset_id` from `FakeEndpointRepo` |
| `tests/integration/repositories/test_sql_endpoint_repository.py` | **Modify** | Update integration tests to use name, no asset_id |
| `tests/integration/repositories/test_sql_asset_repository.py` | **Modify** | Add tests for new repo methods |
| `tests/integration/test_discovery_api.py` | **Modify** | Fix integration tests to use names and correct DB setups |

---

## Task 1: Refactor Endpoint Domain, DB Model, and Repository

**Files:**
- Modify: `app/domain/endpoints/endpoint.py`
- Modify: `app/infrastructure/persistence/models/endpoint_model.py`
- Modify: `app/domain/endpoints/endpoint_repository.py`
- Modify: `app/infrastructure/persistence/repositories/sql_endpoint_repository.py`
- Modify: `tests/unit/domain/endpoints/test_endpoint_service.py`

**Interfaces:**
- Consumes: None (starting layer)
- Produces: Updated `Endpoint` dataclass, updated `EndpointModel`, `find_by_name` on `EndpointRepository`

- [ ] **Step 1: Write the failing test**

Modify `tests/unit/domain/endpoints/test_endpoint_service.py` to:
1. Remove `asset_id` from constructor calls.
2. Add `name: str` to constructor calls.
3. Update `FakeEndpointRepository` to replace `find_by_asset_id` with `find_by_name`.
4. Delete `test_find_for_asset_returns_none_when_not_provisioned`.

```python
# Modified FakeEndpointRepository in tests/unit/domain/endpoints/test_endpoint_service.py
class FakeEndpointRepository:
    def __init__(self) -> None:
        self._store: dict[str, AnyEndpoint] = {}

    async def save(self, endpoint: AnyEndpoint) -> AnyEndpoint:
        self._store[endpoint.id] = endpoint
        return endpoint

    async def find_by_id(self, endpoint_id: str) -> AnyEndpoint | None:
        return self._store.get(endpoint_id)

    async def find_by_name(self, name: str) -> AnyEndpoint | None:
        return next((e for e in self._store.values() if e.name == name), None)
```

Example failing test update:
```python
# tests/unit/domain/endpoints/test_endpoint_service.py
@pytest.mark.asyncio
async def test_provision_database_endpoint_has_typed_fields() -> None:
    service = EndpointService(repo=FakeEndpointRepository())
    ep = DatabaseEndpoint(
        id=_id(),
        name="pg-db",
        credential_ref=_cred(),
    )
    saved = await service.provision(ep)
    assert isinstance(saved, DatabaseEndpoint)
    assert saved.type == EndpointType.DATABASE
    assert saved.name == "pg-db"
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/unit/domain/endpoints/test_endpoint_service.py`
Expected: FAIL (Compilation/Import error because `Endpoint` takes `asset_id` and lacks `name`)

- [ ] **Step 3: Implement Endpoint domain, model, and repository**

Modify `app/domain/endpoints/endpoint.py`:
```python
@dataclass(kw_only=True)
class Endpoint(ABC, Auditable):
    id: str
    name: str  # Added
    credential_ref: CredentialReference
    technical_description: str = ""
    # Removed asset_id
```

Modify `app/infrastructure/persistence/models/endpoint_model.py`:
```python
class EndpointModel(Base, TimestampMixin):
    __tablename__ = "endpoints"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)  # Added
    type: Mapped[str] = mapped_column(String(50), nullable=False)
    credential_ref: Mapped[str] = mapped_column(String(500), nullable=False)
    technical_description: Mapped[str] = mapped_column(String(2000), nullable=False, default="")
    subtype_data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    # Removed asset_id column
```

Modify `app/domain/endpoints/endpoint_repository.py`:
```python
@runtime_checkable
class EndpointRepository(Protocol):
    async def save(self, endpoint: AnyEndpoint) -> AnyEndpoint: ...
    async def find_by_id(self, endpoint_id: str) -> AnyEndpoint | None: ...
    async def find_by_name(self, name: str) -> AnyEndpoint | None: ... # Added
    # Removed find_by_asset_id
```

Modify `app/infrastructure/persistence/repositories/sql_endpoint_repository.py`:
```python
_BASE_FIELDS = {
    "id",
    "name",  # Changed from asset_id to name
    "credential_ref",
    "technical_description",
    "type",
    "created_at",
    "updated_at",
}

def _to_domain(m: EndpointModel) -> AnyEndpoint:
    base: dict[str, Any] = {
        "id": m.id,
        "name": m.name,  # Changed
        "credential_ref": CredentialReference(m.credential_ref),
        "technical_description": m.technical_description,
        "created_at": m.created_at,
        "updated_at": m.updated_at,
        **m.subtype_data,
    }
    # ...

def _to_model(endpoint: AnyEndpoint) -> EndpointModel:
    all_fields = {
        k: v for k, v in vars(endpoint).items() if k not in _BASE_FIELDS and not k.startswith("_")
    }
    return EndpointModel(
        id=endpoint.id,
        name=endpoint.name,  # Changed
        type=endpoint.type.value,
        credential_ref=endpoint.credential_ref.path,
        technical_description=endpoint.technical_description,
        subtype_data=all_fields,
    )

# Implement find_by_name and remove find_by_asset_id in SqlEndpointRepository
    async def find_by_name(self, name: str) -> AnyEndpoint | None:
        result = await self._session.execute(
            select(EndpointModel).where(EndpointModel.name == name)
        )
        model = result.scalar_one_or_none()
        return _to_domain(model) if model else None

    # DELETE the entire find_by_asset_id method:
    # async def find_by_asset_id(self, asset_id: str) -> AnyEndpoint | None:
    #     ...  ← apagar completamente
```

Modify `app/domain/endpoints/endpoint_service.py`:
```python
# Remove find_for_asset method entirely (it delegates to find_by_asset_id which no longer exists)
```

Modify `tests/unit/application/test_unit_of_work.py` — update `FakeEndpointRepo`:
```python
class FakeEndpointRepo:
    def __init__(self) -> None:
        self._store: dict[str, object] = {}

    async def save(self, endpoint: object) -> object:
        return endpoint

    async def find_by_id(self, endpoint_id: str) -> object | None:
        return None

    # find_by_asset_id REMOVED — replaced by find_by_name
    async def find_by_name(self, name: str) -> object | None:
        return None
```

- [ ] **Step 4: Run to verify it passes**

Run: `pytest tests/unit/domain/endpoints/test_endpoint_service.py tests/unit/application/test_unit_of_work.py`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/domain/endpoints/endpoint.py \
        app/infrastructure/persistence/models/endpoint_model.py \
        app/domain/endpoints/endpoint_repository.py \
        app/infrastructure/persistence/repositories/sql_endpoint_repository.py \
        app/domain/endpoints/endpoint_service.py \
        tests/unit/domain/endpoints/test_endpoint_service.py \
        tests/unit/application/test_unit_of_work.py
git commit -m "feat: refactor endpoint entity and repository to use name instead of asset_id"
```

---

## Task 2: Refactor Asset Repository and Domain Service

**Files:**
- Modify: `app/domain/assets/asset_repository.py`
- Modify: `app/infrastructure/persistence/repositories/sql_asset_repository.py`
- Modify: `app/domain/assets/asset_service.py`

**Interfaces:**
- Consumes: None (starting layer)
- Produces: Updated repository protocol with `find_by_name` and `update`, updated domain service `update` method

- [ ] **Step 1: Write the failing test**

Modify `tests/integration/repositories/test_sql_asset_repository.py` to add tests for `find_by_name` and `update`:

```python
# Add to tests/integration/repositories/test_sql_asset_repository.py
@pytest.mark.asyncio
async def test_find_by_name(db_session: AsyncSession) -> None:
    repo = SqlAssetRepository(db_session)
    asset = await repo.save(_asset(name="my-unique-asset"))
    found = await repo.find_by_name("my-unique-asset")
    assert found is not None
    assert found.id == asset.id

@pytest.mark.asyncio
async def test_update_asset(db_session: AsyncSession) -> None:
    repo = SqlAssetRepository(db_session)
    asset = await repo.save(_asset())
    asset.description = "New description"
    asset.tags = ["new-tag"]
    updated = await repo.update(asset)
    assert updated.description == "New description"
    assert updated.tags == ["new-tag"]
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/integration/repositories/test_sql_asset_repository.py`
Expected: FAIL (`AttributeError: 'SqlAssetRepository' object has no attribute 'find_by_name'`)

- [ ] **Step 3: Implement repository modifications and update method**

Modify `app/domain/assets/asset_repository.py`:
```python
@runtime_checkable
class AssetRepository(Protocol):
    async def save(self, asset: DataAsset) -> DataAsset: ...
    async def find_by_id(self, asset_id: str) -> DataAsset | None: ...
    async def find_by_name(self, name: str) -> DataAsset | None: ...  # Added
    async def update_state(self, asset_id: str, new_state: AssetState) -> DataAsset: ...
    async def update_endpoint(self, asset_id: str, endpoint_id: str) -> DataAsset: ...
    async def update_scope(self, asset_id: str, scope: DiscoveryScope) -> DataAsset: ...
    async def update(self, asset: DataAsset) -> DataAsset: ...  # Added
```

Modify `app/infrastructure/persistence/repositories/sql_asset_repository.py`:
```python
    async def find_by_name(self, name: str) -> DataAsset | None:
        result = await self._session.execute(
            select(DataAssetModel).where(DataAssetModel.name == name)
        )
        model = result.scalar_one_or_none()
        return _to_domain(model) if model else None

    async def update(self, asset: DataAsset) -> DataAsset:
        model = await self._fetch_or_raise(asset.id)
        model.description = asset.description
        model.tags = asset.tags
        model.policy_tags = [t.value for t in asset.policy_tags]
        model.endpoint_id = asset.endpoint_id
        await self._session.flush()
        await self._session.refresh(model)
        return _to_domain(model)
```

Modify `app/domain/assets/asset_service.py`:
```python
    async def update(
        self,
        asset_id: str,
        description: str | None = None,
        tags: list[str] | None = None,
        policy_tags: list[PolicyTag] | None = None,
        endpoint_id: str | None = None,
    ) -> DataAsset:
        asset = await self._require_asset(asset_id)
        if description is not None:
            asset.description = description
        if tags is not None:
            asset.tags = tags
        if policy_tags is not None:
            asset.policy_tags = policy_tags
        if endpoint_id is not None:
            asset.endpoint_id = endpoint_id
        return await self._repo.update(asset)
```

- [ ] **Step 4: Run to verify it passes**

Run: `pytest tests/integration/repositories/test_sql_asset_repository.py`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/domain/assets/asset_repository.py \
        app/infrastructure/persistence/repositories/sql_asset_repository.py \
        app/domain/assets/asset_service.py \
        tests/integration/repositories/test_sql_asset_repository.py
git commit -m "feat: add find_by_name and update capabilities to asset repo and service"
```

---

## Task 3: Refactor Provision Endpoint Use Case and Endpoint Router

**Files:**
- Modify: `app/application/endpoints/provision_endpoint.py`
- Modify: `app/infrastructure/http/routers/endpoint_router.py`

**Interfaces:**
- Consumes: `EndpointService`
- Produces: Updated endpoint provisioning HTTP route and use case

- [ ] **Step 1: Write the failing test**

We don't have separate unit tests for `provision_endpoint.py` since it's verified via the HTTP router if integration test is updated, but let's write a unit test to verify it.
Wait, let's create a unit test `tests/unit/application/test_provision_endpoint.py`:

```python
# tests/unit/application/test_provision_endpoint.py
from __future__ import annotations
import pytest
from app.application.endpoints.provision_endpoint import ProvisionEndpointUseCase
from tests.unit.application.test_register_asset import MockUoW

@pytest.mark.asyncio
async def test_provision_database_endpoint() -> None:
    uow = MockUoW()
    use_case = ProvisionEndpointUseCase(uow=uow)
    saved = await use_case.execute_database(
        name="pg-db",
        credential_ref="vault/db/pg",
        technical_description="technical desc"
    )
    assert saved.name == "pg-db"
    assert uow.commit_called is True
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/unit/application/test_provision_endpoint.py`
Expected: FAIL (`TypeError: execute_database() missing 1 required positional argument: 'asset_id'`)

- [ ] **Step 3: Implement Provision Endpoint changes**

Modify `app/application/endpoints/provision_endpoint.py`:
```python
    async def execute_database(
        self, name: str, credential_ref: str, technical_description: str
    ) -> DatabaseEndpoint:
        import uuid
        from app.domain.shared.value_objects import CredentialReference
        from app.domain.endpoints.endpoint import DatabaseEndpoint
        
        ep = DatabaseEndpoint(
            id=str(uuid.uuid4()),
            name=name,  # Changed from asset_id to name
            credential_ref=CredentialReference(credential_ref),
            technical_description=technical_description,
        )
        async with self._uow:
            service = EndpointService(repo=self._uow.endpoints)
            saved = await service.provision(ep)
            await self._uow.commit()
        return saved
```

Modify `app/infrastructure/http/routers/endpoint_router.py`:
```python
class EndpointResponse(BaseModel):
    id: str
    name: str  # Changed from asset_id to name
    type: EndpointType

class DatabaseEndpointCreateRequest(BaseModel):
    name: str  # Changed from asset_id to name
    credential_ref: str
    technical_description: str = ""

@router.post("/database", response_model=EndpointResponse, status_code=status.HTTP_201_CREATED)
async def provision_database_endpoint(
    body: DatabaseEndpointCreateRequest,
    _: CurrentUser = Depends(require_role(Role.SRE, Role.PO_PM)),
) -> EndpointResponse:
    uow = SqlUnitOfWork(get_session_factory())
    use_case = ProvisionEndpointUseCase(uow=uow)
    
    saved = await use_case.execute_database(
        name=body.name,  # Changed
        credential_ref=body.credential_ref,
        technical_description=body.technical_description
    )
    return EndpointResponse(id=saved.id, name=saved.name, type=saved.type)  # Changed
```

- [ ] **Step 4: Run to verify it passes**

Run: `pytest tests/unit/application/test_provision_endpoint.py`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/application/endpoints/provision_endpoint.py \
        app/infrastructure/http/routers/endpoint_router.py \
        tests/unit/application/test_provision_endpoint.py
git commit -m "feat: make endpoint provisioning independent of assets using names"
```

---

## Task 4: Refactor Activate Use Case and Schemas

> **Princípio de Arquitetura (Clean Architecture):** Use Cases da camada Application trabalham **apenas com IDs internos**. A tradução de nomes amigáveis (HTTP) para IDs é responsabilidade exclusiva dos **Routers (Infrastructure)**. Isso mantém os Use Cases portáveis para mensageria (Kafka, CLI, Scheduler) sem dependência do contexto HTTP.

**Files:**
- Modify: `app/infrastructure/http/schemas/asset_schemas.py`
- Modify: `app/application/assets/activate_asset.py`
- Modify: `tests/unit/application/test_activate_asset.py`

**Interfaces:**
- Consumes: `AssetRepository`, `EndpointRepository`
- Produces: Updated activate use case (recebe IDs) e asset response schemas

- [ ] **Step 1: Write the failing test**

Update `tests/unit/application/test_activate_asset.py` — o Use Case agora recebe `asset_id` e `endpoint_id` diretamente (IDs resolvidos pelo Router):

```python
# tests/unit/application/test_activate_asset.py
@pytest.mark.asyncio
async def test_activate_asset_calls_adapters_after_commit():
    uow = MockUoW()
    catalog = AsyncMock(spec=NoopCatalogAdapter)
    notifications = AsyncMock(spec=NoopNotificationAdapter)

    active_asset = DataAsset(
        id="a1",
        name="test-asset",
        description="desc",
        owner=EmailAddress("t@co.com"),
        tags=[],
        policy_tags=[],
        state=AssetState.ACTIVE,
        discovery_schedule=CronSchedule("0 * * * *"),
        discovery_scope=DiscoveryScope(),
        endpoint_id="ep1",
    )
    uow.assets.update_endpoint.return_value = None
    uow.assets.update_state.return_value = active_asset

    use_case = ActivateAssetUseCase(uow=uow, catalog=catalog, notifications=notifications)

    # Use case recebe IDs — name resolution é feita pelo Router antes desta chamada
    asset = await use_case.execute(asset_id="a1", endpoint_id="ep1")

    assert uow.commit_called is True
    catalog.publish_asset.assert_called_once_with(
        asset_id=asset.id, name=asset.name, state="active", metadata={"endpoint_id": "ep1"}
    )
    notifications.send_alert.assert_called_once()
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/unit/application/test_activate_asset.py`
Expected: FAIL (assinatura do `execute` ainda aceita nomes, não IDs)

- [ ] **Step 3: Modify Use Case and Schemas**

Modify `app/infrastructure/http/schemas/asset_schemas.py`:
```python
# app/infrastructure/http/schemas/asset_schemas.py
class AssetResponse(BaseModel):
    model_config = ConfigDict(from_attributes=False)

    id: str
    name: str
    description: str
    owner_email: str
    tags: list[str]
    policy_tags: list[PolicyTag]
    state: AssetState
    discovery_schedule: str
    discovery_scope_include: list[str]
    discovery_scope_exclude: list[str]
    # Mantém endpoint_id (ID interno) na response — inputs usam nomes, outputs usam IDs
    endpoint_id: str | None
```

Modify `app/application/assets/activate_asset.py` — assinatura muda para receber IDs:
```python
    async def execute(self, asset_id: str, endpoint_id: str) -> DataAsset:
        """Transitions a DataAsset to ACTIVE state, linking it to an Endpoint.

        Name resolution (asset_name -> asset_id, endpoint_name -> endpoint_id)
        is the Router's responsibility. This Use Case works only with IDs.
        """
        async with self._uow:
            service = AssetService(repo=self._uow.assets)
            updated = await service.transition_to_active(asset_id, endpoint_id)
            await self._uow.commit()

        await self._catalog.publish_asset(
            asset_id=updated.id,
            name=updated.name,
            state=updated.state.value,
            metadata={"endpoint_id": endpoint_id},
        )
        await self._notifications.send_alert(
            channel="#data-platform",
            title="Data Asset Activated",
            message=f"Asset {updated.name} is now ACTIVE.",
            level="info",
        )
        return updated
```

- [ ] **Step 4: Run to verify it passes**

Run: `pytest tests/unit/application/test_activate_asset.py`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/infrastructure/http/schemas/asset_schemas.py \
        app/application/assets/activate_asset.py \
        tests/unit/application/test_activate_asset.py
git commit -m "refactor: activate use case receives IDs, name resolution moved to Router layer"
```

---

## Task 5: Implement Update Asset Use Case

> **Princípio de Arquitetura (Clean Architecture):** O `UpdateAssetUseCase` recebe `asset_id: str` e `endpoint_id: str | None` — apenas IDs. A resolução de `asset_name` e `endpoint_name` é feita no Router (Task 6) antes de chamar o Use Case. Isso mantém o Use Case agnóstico ao protocolo de transporte.

**Files:**
- Create: `app/application/assets/update_asset.py`
- Modify: `app/infrastructure/http/schemas/asset_schemas.py`
- Create: `tests/unit/application/test_update_asset.py`

**Interfaces:**
- Consumes: `AssetRepository`, `CatalogAdapter`, `NotificationAdapter`
- Produces: `UpdateAssetUseCase` com assinatura baseada em IDs internos

- [ ] **Step 1: Write the failing test**

Create `tests/unit/application/test_update_asset.py`:
```python
# tests/unit/application/test_update_asset.py
from __future__ import annotations
from unittest.mock import AsyncMock
import pytest
from app.application.assets.update_asset import UpdateAssetUseCase
from app.domain.assets.asset_state import AssetState
from app.domain.assets.data_asset import DataAsset
from app.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress
from app.infrastructure.adapters.catalog.noop_adapter import NoopCatalogAdapter
from app.infrastructure.adapters.notifications.noop_notification_adapter import NoopNotificationAdapter
from tests.unit.application.test_register_asset import MockUoW

@pytest.mark.asyncio
async def test_update_asset_success():
    uow = MockUoW()
    catalog = AsyncMock(spec=NoopCatalogAdapter)
    notifications = AsyncMock(spec=NoopNotificationAdapter)
    
    asset = DataAsset(
        id="a1",
        name="test-asset",
        description="old desc",
        owner=EmailAddress("t@co.com"),
        tags=["core"],
        policy_tags=[],
        state=AssetState.ACTIVE,
        discovery_schedule=CronSchedule("0 * * * *"),
        discovery_scope=DiscoveryScope()
    )
    uow.assets.update.return_value = asset

    use_case = UpdateAssetUseCase(uow=uow, catalog=catalog, notifications=notifications)
    # Use case recebe IDs — o Router (Task 6) já resolveu asset_name → asset_id
    updated = await use_case.execute(
        asset_id="a1",
        description="new desc",
        tags=["reporting"],
        endpoint_id="ep1",  # ID já resolvido pelo Router
    )
    
    assert uow.commit_called is True
    catalog.publish_asset.assert_called_once()
    notifications.send_alert.assert_called_once()
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/unit/application/test_update_asset.py`
Expected: FAIL (`ImportError: cannot import name 'UpdateAssetUseCase'`)

- [ ] **Step 3: Implement update schema and Use Case**

Add to `app/infrastructure/http/schemas/asset_schemas.py`:
```python
class AssetUpdateRequest(BaseModel):
    description: str | None = None
    tags: list[str] | None = None
    policy_tags: list[PolicyTag] | None = None
    # Usuário informa nome do endpoint; o Router resolve para ID antes de chamar o Use Case
    endpoint_name: str | None = None
```

Create `app/application/assets/update_asset.py`:
```python
from __future__ import annotations

from app.application.unit_of_work import UnitOfWork
from app.domain.assets.asset_service import AssetService
from app.domain.assets.data_asset import DataAsset
from app.domain.shared.policy_tag import PolicyTag
from app.application.shared.adapters.catalog_adapter import CatalogAdapter
from app.infrastructure.adapters.notifications.notification_adapter import NotificationAdapter


class UpdateAssetUseCase:
    """Updates DataAsset fields and publishes the metadata delta to the catalog.

    Receives internal IDs only. Name resolution (asset_name -> asset_id,
    endpoint_name -> endpoint_id) is the Router's responsibility.
    """

    def __init__(
        self, uow: UnitOfWork, catalog: CatalogAdapter, notifications: NotificationAdapter
    ) -> None:
        self._uow = uow
        self._catalog = catalog
        self._notifications = notifications

    async def execute(
        self,
        asset_id: str,
        description: str | None = None,
        tags: list[str] | None = None,
        policy_tags: list[PolicyTag] | None = None,
        endpoint_id: str | None = None,
    ) -> DataAsset:
        async with self._uow:
            service = AssetService(repo=self._uow.assets)
            updated = await service.update(
                asset_id,
                description=description,
                tags=tags,
                policy_tags=policy_tags,
                endpoint_id=endpoint_id,
            )
            await self._uow.commit()

        await self._catalog.publish_asset(
            asset_id=updated.id,
            name=updated.name,
            state=updated.state.value,
            metadata={"endpoint_id": updated.endpoint_id},
        )
        await self._notifications.send_alert(
            channel="#data-platform",
            title="Data Asset Updated",
            message=f"Asset {updated.name} was successfully updated.",
            level="info",
        )
        return updated
```

- [ ] **Step 4: Run to verify it passes**

Run: `pytest tests/unit/application/test_update_asset.py`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/application/assets/update_asset.py \
        tests/unit/application/test_update_asset.py \
        app/infrastructure/http/schemas/asset_schemas.py
git commit -m "feat: implement update asset use case with ID-based interface"
```

---

## Task 6: Refactor Asset and Discovery Routers to Name-based routing

**Files:**
- Modify: `app/infrastructure/http/routers/asset_router.py`
- Modify: `app/infrastructure/http/routers/discovery_router.py`

**Interfaces:**
- Consumes: `UpdateAssetUseCase`, `ActivateAssetUseCase`, `RegisterAssetUseCase`
- Produces: Updated FastAPI endpoints routing on `asset_name`

- [ ] **Step 1: Write the failing test**

Modify `tests/integration/test_discovery_api.py` and `tests/integration/repositories/test_sql_endpoint_repository.py` to fix schema and route naming:

1. Update `tests/integration/repositories/test_sql_endpoint_repository.py`:
   - Replace `asset_id="asset-1"` with `name="ep1"`.
   - Replace `asset_id="asset-2"` with `name="ep2"`.

2. Update `tests/integration/test_discovery_api.py`:
   - Fix `EndpointModel` creation: remove `asset_id` and add `name="ep-1"`.
   - Update router endpoints called: `/discovery/assets/test-asset/run` (using the asset's name `test-asset` instead of its ID `asset-1`).

```python
# Example in tests/integration/test_discovery_api.py
@pytest.mark.asyncio
async def test_trigger_discovery_run_success(po_pm_client: AsyncClient, db_session) -> None:
    # 1. Setup Data
    endpoint = EndpointModel(
        id="ep-1",
        name="ep-main",  # Added name, removed asset_id
        type="database",
        credential_ref="secret",
        technical_description="",
        subtype_data={}
    )
    db_session.add(endpoint)
    # ...
    # 2. Execute
    response = await po_pm_client.post(
        "/discovery/assets/test-asset/run",  # Changed from /discovery/assets/asset-1/run
        json={"triggered_by": "manual_test"}
    )
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/integration/test_discovery_api.py`
Expected: FAIL (`404 Not Found` or `422 Unprocessable Entity` because routers expect asset ID)

- [ ] **Step 3: Modify routers to take name instead of ID**

> **Clean Architecture:** Os Routers são o único ponto onde nomes (HTTP-friendly) são resolvidos para IDs internos. Os Use Cases recebem apenas IDs. Isso permite reutilizar os Use Cases via CLI, Kafka ou agendador sem alterações.

Modify `app/infrastructure/http/routers/asset_router.py`:
```python
# Change GET /{asset_id} to GET /{asset_name}
@router.get("/{asset_name}", response_model=AssetResponse)
async def get_asset(
    asset_name: str,
    session: AsyncSession = Depends(get_db),
    _: CurrentUser = Depends(get_current_user),
) -> AssetResponse:
    from app.infrastructure.persistence.repositories.sql_asset_repository import SqlAssetRepository
    repo = SqlAssetRepository(session)
    asset = await repo.find_by_name(asset_name)
    if asset is None:
        raise HTTPException(status_code=404, detail=f"Asset not found: {asset_name!r}")
    return asset_to_response(asset)

# Change activate_asset — Router resolve nomes para IDs antes de chamar o Use Case
@router.post("/{asset_name}/activate", response_model=AssetResponse)
async def activate_asset(
    asset_name: str,
    endpoint_name: str,
    session: AsyncSession = Depends(get_db),
    _: CurrentUser = Depends(require_role(Role.SRE)),
) -> AssetResponse:
    # 1. Resolver nomes → IDs (responsabilidade do Router)
    from app.infrastructure.persistence.repositories.sql_asset_repository import SqlAssetRepository
    from app.infrastructure.persistence.repositories.sql_endpoint_repository import SqlEndpointRepository
    asset_repo = SqlAssetRepository(session)
    endpoint_repo = SqlEndpointRepository(session)

    asset = await asset_repo.find_by_name(asset_name)
    if not asset:
        raise HTTPException(status_code=404, detail=f"Asset not found: {asset_name!r}")
    endpoint = await endpoint_repo.find_by_name(endpoint_name)
    if not endpoint:
        raise HTTPException(status_code=404, detail=f"Endpoint not found: {endpoint_name!r}")

    # 2. Passar IDs para o Use Case
    uow = SqlUnitOfWork(get_session_factory())
    use_case = ActivateAssetUseCase(
        uow=uow, catalog=get_catalog_adapter(get_settings()), notifications=NoopNotificationAdapter()
    )
    try:
        asset = await use_case.execute(asset_id=asset.id, endpoint_id=endpoint.id)
    except InvalidStateTransitionError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return asset_to_response(asset)

# Add PUT /{asset_name} — Router resolve nomes → IDs antes de chamar UpdateAssetUseCase
@router.put("/{asset_name}", response_model=AssetResponse)
async def update_asset(
    asset_name: str,
    body: AssetUpdateRequest,
    session: AsyncSession = Depends(get_db),
    _: CurrentUser = Depends(require_role(Role.PO_PM, Role.SRE)),
) -> AssetResponse:
    # 1. Resolver asset_name → asset_id
    from app.infrastructure.persistence.repositories.sql_asset_repository import SqlAssetRepository
    from app.infrastructure.persistence.repositories.sql_endpoint_repository import SqlEndpointRepository
    asset_repo = SqlAssetRepository(session)
    asset = await asset_repo.find_by_name(asset_name)
    if not asset:
        raise HTTPException(status_code=404, detail=f"Asset not found: {asset_name!r}")

    # 2. Resolver endpoint_name → endpoint_id (se fornecido)
    endpoint_id: str | None = None
    if body.endpoint_name:
        endpoint_repo = SqlEndpointRepository(session)
        endpoint = await endpoint_repo.find_by_name(body.endpoint_name)
        if not endpoint:
            raise HTTPException(status_code=404, detail=f"Endpoint not found: {body.endpoint_name!r}")
        endpoint_id = endpoint.id

    # 3. Passar IDs para o Use Case
    uow = SqlUnitOfWork(get_session_factory())
    use_case = UpdateAssetUseCase(
        uow=uow, catalog=get_catalog_adapter(get_settings()), notifications=NoopNotificationAdapter()
    )
    try:
        asset = await use_case.execute(
            asset_id=asset.id,
            description=body.description,
            tags=body.tags,
            policy_tags=[PolicyTag(t) for t in body.policy_tags] if body.policy_tags else None,
            endpoint_id=endpoint_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return asset_to_response(asset)
```

Modify `app/infrastructure/http/routers/discovery_router.py`:
```python
@router.post("/assets/{asset_name}/run", response_model=DiscoveryRunResponse, status_code=status.HTTP_201_CREATED)
async def trigger_discovery_run(
    asset_name: str,
    body: TriggerDiscoveryRequest,
    session: AsyncSession = Depends(get_db),
    _: CurrentUser = Depends(require_role(Role.PO_PM, Role.ANALYTICS_ENGINEER, Role.SRE)),
) -> DiscoveryRunResponse:
    # Resolve asset_name to asset_id
    from app.infrastructure.persistence.repositories.sql_asset_repository import SqlAssetRepository
    repo = SqlAssetRepository(session)
    asset = await repo.find_by_name(asset_name)
    if not asset:
        raise HTTPException(status_code=404, detail=f"Asset not found: {asset_name}")

    uow = SqlUnitOfWork(get_session_factory())
    secret_manager = get_secret_manager(get_settings())
    factory = DiscoveryRunnerFactoryImpl(secret_manager=secret_manager)
    
    use_case = RunDiscoveryUseCase(
        uow=uow,
        runner_factory=factory,
        schema_differ=SchemaDiffer(),
        tag_inferrer=PolicyTagInferrer(),
    )
    
    try:
        run = await use_case.execute(asset_id=asset.id, triggered_by=body.triggered_by)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
        
    return DiscoveryRunResponse.model_validate(run)
```

- [ ] **Step 4: Run to verify it passes**

Run: `pytest tests/integration/test_discovery_api.py`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/infrastructure/http/routers/asset_router.py \
        app/infrastructure/http/routers/discovery_router.py \
        tests/integration/test_discovery_api.py \
        tests/integration/repositories/test_sql_endpoint_repository.py
git commit -m "feat: complete transition of routers and tests to name-based API paths"
```

---

## Verification Plan

### Automated Tests
- Run all unit and integration tests:
  ```bash
  pytest tests/ -v
  ```

### Manual Verification
- Launch local environment using docker-compose.
- Provision an endpoint:
  ```bash
  curl -X POST http://localhost:8000/endpoints/database \
    -H "Content-Type: application/json" \
    -d '{"name": "postgres-prod", "credential_ref": "vault/secret/postgres"}'
  ```
- Register a DataAsset:
  ```bash
  curl -X POST http://localhost:8000/assets \
    -H "Content-Type: application/json" \
    -d '{"name": "sales-ledger", "description": "Sales logs", "owner_email": "po@co.com", "discovery_schedule": "0 6 * * *"}'
  ```
- Activate the asset linking to the provisioned endpoint:
  ```bash
  curl -X POST "http://localhost:8000/assets/sales-ledger/activate?endpoint_name=postgres-prod"
  ```
- Update asset fields and endpoint link:
  ```bash
  curl -X PUT http://localhost:8000/assets/sales-ledger \
    -H "Content-Type: application/json" \
    -d '{"description": "Updated sales logs desc", "tags": ["finance", "audit"]}'
  ```
- Verify get by name:
  ```bash
  curl -H "Accept: application/json" http://localhost:8000/assets/sales-ledger
  ```
