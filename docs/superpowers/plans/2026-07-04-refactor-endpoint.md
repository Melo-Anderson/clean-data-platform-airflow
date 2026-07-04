# Refactor Endpoint Connection Info to Vault Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove connection properties (`host`, `port`, `database`, `driver`) from the `DatabaseEndpoint` entity, as they should be kept in the Vault (referenced by `credential_ref`). Update registration logic and schema validation to allow both `Role.PO_PM` and `Role.SRE` to provision endpoints.

**Architecture (with Clean Architecture improvements):** 
- Modify the domain `DatabaseEndpoint` dataclass to remove connection properties.
- Update `DatabaseEndpointCreateRequest` schema in the API layer.
- Refactor `ProvisionEndpointUseCase` to encapsulate Entity creation (preventing domain logic leak into the Router).
- Refactor `provision_database_endpoint` router endpoint to allow `Role.PO_PM` and `Role.SRE`, and use `SqlUnitOfWork` with `ProvisionEndpointUseCase` instead of manual instantiation.
- Update docstrings to reflect new entity shapes (Clean Code).
- Update unit and integration tests.

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, pytest.

## Global Constraints

- No DB schema changes are needed since dynamic subtype fields are already stored in a JSON column (`subtype_data`).
- Maintain code compliance with Clean Architecture and Clean Code guidelines.

---

### Task 1: Refactor DatabaseEndpoint Domain Entity

**Files:**
- Modify: `app/domain/endpoints/endpoint.py:33-54`

**Interfaces:**
- Consumes: None
- Produces: Simplified `DatabaseEndpoint` class.

- [ ] **Step 1: Modify `DatabaseEndpoint` class in `endpoint.py`**

Open [endpoint.py](file:///C:/Users/natha/OneDrive/Documentos/Estudo/airflow-data-platform-sdd/app/domain/endpoints/endpoint.py#L33-L54) and modify `DatabaseEndpoint` to remove `host`, `port`, `database`, and `driver` fields.

```python
@dataclass(kw_only=True)
class DatabaseEndpoint(Endpoint):
    """
    Endpoint for relational databases (Oracle, PostgreSQL, MySQL, etc.).

    All connection details (host, port, database, driver) reside in the Vault
    and are resolved dynamically via credential_ref.
    """

    @property
    def type(self) -> EndpointType:
        return EndpointType.DATABASE
```

- [ ] **Step 2: Update `EndpointService` docstring (Clean Code)**

Open [endpoint_service.py](file:///C:/Users/natha/OneDrive/Documentos/Estudo/airflow-data-platform-sdd/app/domain/endpoints/endpoint_service.py#L32) and remove connection properties from the docstring example.

```python
        Example:
            ep = DatabaseEndpoint(id="uuid", asset_id="uuid", credential_ref=CredentialReference("..."))
            saved = await service.provision(ep)
```

- [ ] **Step 3: Run pytest to verify domain model updates break only endpoint tests**

Run: `uv run pytest tests/ -v`
Expected: Only tests involving `DatabaseEndpoint` should fail.

---

### Task 2: Refactor Application Layer (Clean Architecture)

**Files:**
- Modify: `app/application/endpoints/provision_endpoint.py:17-23`

**Interfaces:**
- Consumes: `DatabaseEndpoint`
- Produces: Updated `ProvisionEndpointUseCase` with a dedicated method for database endpoints.

- [ ] **Step 1: Add `execute_database` method to encapsulate entity creation**

Open [provision_endpoint.py](file:///C:/Users/natha/OneDrive/Documentos/Estudo/airflow-data-platform-sdd/app/application/endpoints/provision_endpoint.py#L17-L23) and add a specific method to construct the entity, generate the UUID, and save it. This prevents the HTTP layer from knowing how to build domain entities.

```python
    import uuid
    from app.domain.shared.value_objects import CredentialReference
    from app.domain.endpoints.endpoint import DatabaseEndpoint

    async def execute_database(
        self, asset_id: str, credential_ref: str, technical_description: str
    ) -> DatabaseEndpoint:
        ep = DatabaseEndpoint(
            id=str(uuid.uuid4()),
            asset_id=asset_id,
            credential_ref=CredentialReference(credential_ref),
            technical_description=technical_description,
        )
        async with self._uow:
            service = EndpointService(repo=self._uow.endpoints)
            saved = await service.provision(ep)
            await self._uow.commit()
        return saved
```

---

### Task 3: Refactor API router and schemas

**Files:**
- Modify: `app/infrastructure/http/routers/endpoint_router.py:34-63`

**Interfaces:**
- Consumes: `ProvisionEndpointUseCase` (from Task 2)
- Produces: Updated Swagger schema and cleaner router endpoint.

- [ ] **Step 1: Simplify `DatabaseEndpointCreateRequest` and update role checks**

Open [endpoint_router.py](file:///C:/Users/natha/OneDrive/Documentos/Estudo/airflow-data-platform-sdd/app/infrastructure/http/routers/endpoint_router.py#L34-L63).
1. Remove `host`, `port`, `database`, and `driver` fields from `DatabaseEndpointCreateRequest`.
2. Update the role dependency in `provision_database_endpoint` to allow `Role.SRE` and `Role.PO_PM`.
3. Use `SqlUnitOfWork` and `ProvisionEndpointUseCase` instead of instantiating `SqlEndpointRepository` and `DatabaseEndpoint` directly.

```python
from app.infrastructure.persistence.database import get_session_factory
from app.infrastructure.persistence.sql_unit_of_work import SqlUnitOfWork
from app.application.endpoints.provision_endpoint import ProvisionEndpointUseCase

class DatabaseEndpointCreateRequest(BaseModel):
    asset_id: str
    credential_ref: str
    technical_description: str = ""


@router.post("/database", response_model=EndpointResponse, status_code=status.HTTP_201_CREATED)
async def provision_database_endpoint(
    body: DatabaseEndpointCreateRequest,
    _: CurrentUser = Depends(require_role(Role.SRE, Role.PO_PM)),
) -> EndpointResponse:
    """Provision a DatabaseEndpoint. SRE and PO_PM allowed."""
    uow = SqlUnitOfWork(get_session_factory())
    use_case = ProvisionEndpointUseCase(uow=uow)
    
    saved = await use_case.execute_database(
        asset_id=body.asset_id,
        credential_ref=body.credential_ref,
        technical_description=body.technical_description
    )
    return EndpointResponse(id=saved.id, asset_id=saved.asset_id, type=saved.type)
```

---

### Task 4: Refactor and Fix Tests

**Files:**
- Modify: `tests/unit/domain/endpoints/test_endpoint_service.py:80-95`
- Modify: `tests/integration/repositories/test_sql_endpoint_repository.py:22-38`
- Modify: `tests/integration/test_discovery_api.py` (if any tests mock endpoint creation)

- [ ] **Step 1: Update unit tests in `test_endpoint_service.py`**

Open [test_endpoint_service.py](file:///C:/Users/natha/OneDrive/Documentos/Estudo/airflow-data-platform-sdd/tests/unit/domain/endpoints/test_endpoint_service.py#L80-L95) and simplify `DatabaseEndpoint` instantiation.

```python
    ep = DatabaseEndpoint(
        id=_id(),
        asset_id="a1",
        credential_ref=_cred(),
    )
    saved = await service.provision(ep)
    assert isinstance(saved, DatabaseEndpoint)
    assert saved.type == EndpointType.DATABASE
```

- [ ] **Step 2: Update integration tests in `test_sql_endpoint_repository.py`**

Open [test_sql_endpoint_repository.py](file:///C:/Users/natha/OneDrive/Documentos/Estudo/airflow-data-platform-sdd/tests/integration/repositories/test_sql_endpoint_repository.py#L22-L38) and simplify `DatabaseEndpoint` instantiation.

```python
    repo = SqlEndpointRepository(db_session)
    ep = DatabaseEndpoint(
        id=str(uuid.uuid4()),
        asset_id="asset-1",
        credential_ref=_cred(),
    )
    saved = await repo.save(ep)
    assert isinstance(saved, DatabaseEndpoint)
    assert saved.type == EndpointType.DATABASE
```

- [ ] **Step 3: Run the full test suite**

Run: `uv run pytest tests/ -v`
Expected: 127/127 tests passed.
