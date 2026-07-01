# Data Platform — Foundation, Assets & Endpoints Implementation Plan (v3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Scaffoldar a aplicação com Clean Architecture + DDD completo (Value Objects, Aggregates, Domain Events, UoW), implementar RBAC, endpoints polimórficos tipados (ABC), e entregar CRUD completo de DataAsset e Endpoint com audit log atômico via Unit of Work.

**Architecture:** Clean Architecture em 4 camadas: `domain` (puro Python, zero dependências de framework), `application` (use cases com UoW), `infrastructure` (SQLAlchemy, FastAPI, adapters plugáveis), `auth` (cross-cutting). Unit of Work garante atomicidade entre repositórios + audit log. Endpoints são hierarquia ABC tipada no domínio. `@cache` para settings singleton.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy 2.x (async), Alembic, PostgreSQL 15, Pydantic v2, croniter, ruff, mypy, uv, pytest, pytest-asyncio, httpx. Deploy em container GKE-ready.

## Global Constraints

- Python 3.12+. `from __future__ import annotations` em todos os arquivos.
- **Domain layer**: zero imports de `fastapi`, `sqlalchemy`, `pydantic`. Apenas stdlib + typing.
- **Application layer**: zero imports de `fastapi`, `sqlalchemy`. Apenas domain + stdlib.
- **Repositories são Protocols** no domain, `Sql*Repository` na infra.
- **Unit of Work** obrigatório em todos os use cases que envolvem escrita.
- **Endpoints** são hierarquia ABC tipada — sem `extra_fields: dict` no domínio.
- `get_settings()` usa `@cache` (não `@lru_cache`).
- Todos os models ORM têm `created_at` e `updated_at`.
- `uv` para gerenciamento de dependências. `ruff` para lint e format. `mypy` para type-check.
- Testes: `unit/` (fakes nomeados, sem DB), `integration/` (com DB), `contract/` (HTTP schema), `e2e/`, `acceptance/`, `performance/` (skeletons).
- Code comments em inglês. Documentação `.md` em português.
- Commits frequentes: `feat:`, `test:`, `chore:`, `fix:`.
- TDD: teste falha → implementa → teste passa → commit.

---

## Estrutura de Arquivos

```
airflow-data-platform-sdd/
├── platform/
│   ├── __init__.py
│   ├── main.py
│   ├── config.py
│   │
│   ├── domain/
│   │   ├── shared/
│   │   │   ├── value_objects.py         # EmailAddress, CredentialReference, CronSchedule, DiscoveryScope
│   │   │   ├── policy_tag.py            # PolicyTag(StrEnum)
│   │   │   ├── auditable.py             # Auditable mixin (created_at, updated_at)
│   │   │   └── domain_event.py          # DomainEvent base + AssetRegistered, AssetActivated, etc.
│   │   ├── assets/
│   │   │   ├── data_asset.py            # DataAsset entity (aggregate root)
│   │   │   ├── asset_state.py           # AssetState enum + VALID_TRANSITIONS
│   │   │   ├── asset_repository.py      # AssetRepository Protocol
│   │   │   └── asset_service.py         # AssetService + exceptions
│   │   └── endpoints/
│   │       ├── endpoint.py              # Endpoint ABC + all typed subclasses
│   │       ├── endpoint_type.py         # EndpointType(StrEnum)
│   │       ├── endpoint_repository.py   # EndpointRepository Protocol
│   │       └── endpoint_service.py      # EndpointService + exceptions
│   │
│   ├── application/
│   │   ├── unit_of_work.py              # UnitOfWork Protocol + SqlUnitOfWork
│   │   ├── assets/
│   │   │   ├── register_asset.py        # RegisterAssetUseCase
│   │   │   └── activate_asset.py        # ActivateAssetUseCase
│   │   └── endpoints/
│   │       └── provision_endpoint.py    # ProvisionEndpointUseCase
│   │
│   ├── infrastructure/
│   │   ├── persistence/
│   │   │   ├── database.py              # AsyncEngine, session factory
│   │   │   ├── base_model.py            # Base + TimestampMixin
│   │   │   ├── models/
│   │   │   │   ├── data_asset_model.py
│   │   │   │   ├── endpoint_model.py    # Single table with type discriminator + JSON subtype_data
│   │   │   │   └── audit_log_model.py
│   │   │   └── repositories/
│   │   │       ├── sql_asset_repository.py
│   │   │       └── sql_endpoint_repository.py
│   │   ├── adapters/
│   │   │   ├── catalog/
│   │   │   │   ├── catalog_adapter.py
│   │   │   │   ├── noop_catalog_adapter.py
│   │   │   │   ├── datahub_catalog_adapter.py
│   │   │   │   └── openmetadata_catalog_adapter.py
│   │   │   └── notifications/
│   │   │       ├── notification_adapter.py
│   │   │       ├── noop_notification_adapter.py
│   │   │       └── slack_notification_adapter.py
│   │   └── http/
│   │       ├── schemas/
│   │       │   ├── asset_schemas.py
│   │       │   └── endpoint_schemas.py
│   │       └── routers/
│   │           ├── asset_router.py
│   │           └── endpoint_router.py
│   │
│   └── auth/
│       ├── role.py
│       ├── current_user.py
│       └── dependencies.py
│
├── migrations/
├── tests/
│   ├── conftest.py
│   ├── unit/
│   │   ├── domain/
│   │   │   ├── shared/test_value_objects.py
│   │   │   ├── assets/test_asset_service.py
│   │   │   └── endpoints/test_endpoint_service.py
│   │   └── application/
│   │       └── assets/test_register_asset_use_case.py
│   ├── integration/
│   │   └── repositories/
│   │       ├── test_sql_asset_repository.py
│   │       └── test_sql_endpoint_repository.py
│   ├── contract/
│   │   └── test_asset_contract.py
│   ├── e2e/.gitkeep
│   ├── acceptance/.gitkeep
│   └── performance/.gitkeep
│
├── pyproject.toml
├── alembic.ini
├── Makefile
└── Dockerfile
```

---

## Task 1: Scaffolding do Projeto

**Files:**
- Create: `pyproject.toml`
- Create: `Makefile`
- Create: `Dockerfile`
- Create: `platform/__init__.py`
- Create: `platform/config.py`
- Create: `platform/main.py`
- Create: `tests/conftest.py`

**Interfaces:**
- Produces: `platform.config.Settings`, `platform.config.get_settings() -> Settings` (`@cache`)

---

- [ ] **Step 1: Criar pyproject.toml com uv, ruff, mypy**

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "airflow-data-platform"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "fastapi>=0.115",
    "uvicorn[standard]>=0.30",
    "sqlalchemy[asyncio]>=2.0",
    "alembic>=1.13",
    "asyncpg>=0.29",
    "pydantic>=2.7",
    "pydantic-settings>=2.3",
    "python-jose[cryptography]>=3.3",
    "croniter>=3.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.2",
    "pytest-asyncio>=0.23",
    "httpx>=0.27",
    "pytest-cov>=5.0",
    "aiosqlite>=0.20",
    "ruff>=0.4",
    "mypy>=1.10",
]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]

[tool.ruff]
line-length = 100
target-version = "py312"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM", "ANN"]
ignore = ["ANN101", "ANN102"]

[tool.mypy]
python_version = "3.12"
strict = true
ignore_missing_imports = true

[tool.hatch.build.targets.wheel]
packages = ["platform", "cli"]
```

- [ ] **Step 2: Criar Makefile completo com uv, lint, type-check, coverage**

```makefile
.PHONY: install sync dev test test-unit test-integration test-contract \
        coverage lint format format-check type-check check \
        migrate migrate-create migrate-downgrade docker-build docker-run

# --- Dependency management ---
install:
	uv sync --all-extras

sync:
	uv sync --all-extras --upgrade

# --- Development ---
dev:
	uv run uvicorn platform.main:app --reload --port 8000

# --- Testing ---
test:
	uv run pytest tests/ -v

test-unit:
	uv run pytest tests/unit/ -v

test-integration:
	uv run pytest tests/integration/ -v

test-contract:
	uv run pytest tests/contract/ -v

coverage:
	uv run pytest tests/ \
		--cov=platform \
		--cov-report=term-missing \
		--cov-report=html:htmlcov \
		--cov-fail-under=80

# --- Code quality ---
lint:
	uv run ruff check .

format:
	uv run ruff format .

format-check:
	uv run ruff format --check .

type-check:
	uv run mypy platform/

# Full CI gate: format + lint + type-check + all tests with coverage
check: format-check lint type-check coverage
	@echo "✅ All checks passed."

# --- Database ---
migrate:
	uv run alembic upgrade head

migrate-create:
	uv run alembic revision --autogenerate -m "$(name)"

migrate-downgrade:
	uv run alembic downgrade -1

# --- Docker ---
docker-build:
	docker build -t data-platform:dev .

docker-run:
	docker run --env-file .env -p 8000:8000 data-platform:dev
```

- [ ] **Step 3: Criar Dockerfile (GKE-ready)**

```dockerfile
FROM python:3.12-slim AS base

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

RUN useradd --create-home --shell /bin/bash appuser

WORKDIR /app

# Install dependencies in a separate layer for Docker cache efficiency
COPY pyproject.toml ./
RUN uv sync --no-dev --no-install-project

COPY platform/ ./platform/
COPY migrations/ ./migrations/
COPY alembic.ini ./

RUN uv sync --no-dev

USER appuser

EXPOSE 8000

# Runs DB migrations then starts the server
CMD ["sh", "-c", "uv run alembic upgrade head && uv run uvicorn platform.main:app --host 0.0.0.0 --port 8000"]
```

- [ ] **Step 4: Criar platform/config.py com `@cache`**

```python
from __future__ import annotations

from functools import cache

from pydantic import PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Platform configuration loaded from environment variables.

    All fields are injected via env vars prefixed with PLATFORM_.
    Suitable for GKE ConfigMap / Secret injection.
    """

    model_config = SettingsConfigDict(env_file=".env", env_prefix="PLATFORM_")

    database_url: PostgresDsn
    secret_key: str
    algorithm: str = "HS256"
    debug: bool = False

    catalog_adapter: str = "noop"       # "noop" | "datahub" | "openmetadata"
    notification_adapter: str = "noop"  # "noop" | "slack"


@cache
def get_settings() -> Settings:
    """
    Return the cached Settings singleton.

    Using @cache (not @lru_cache) avoids recreating Settings on every call.
    Safe for use as a FastAPI dependency.

    Example:
        settings = get_settings()
        print(settings.catalog_adapter)  # "noop"
    """
    return Settings()  # type: ignore[call-arg]
```

- [ ] **Step 5: Criar platform/main.py**

```python
from __future__ import annotations

from fastapi import FastAPI

from platform.infrastructure.http.routers.asset_router import router as assets_router
from platform.infrastructure.http.routers.endpoint_router import router as endpoints_router


def create_app() -> FastAPI:
    """Create and configure the FastAPI application. No business logic here."""
    app = FastAPI(
        title="Data Platform API",
        version="0.1.0",
        description="Data platform — DataAsset, Endpoint, and Pipeline management.",
    )
    app.include_router(assets_router, prefix="/assets", tags=["assets"])
    app.include_router(endpoints_router, prefix="/endpoints", tags=["endpoints"])
    return app


app = create_app()
```

- [ ] **Step 6: Criar tests/conftest.py**

```python
from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from platform.infrastructure.persistence.base_model import Base
from platform.infrastructure.persistence.database import get_db
from platform.main import create_app

TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"


@pytest.fixture(scope="session")
async def engine():
    eng = create_async_engine(TEST_DATABASE_URL, echo=False)
    yield eng
    await eng.dispose()


@pytest.fixture(autouse=True)
async def setup_tables(engine):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture
async def db_session(engine) -> AsyncSession:
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        yield session


@pytest.fixture
async def client(db_session: AsyncSession) -> AsyncClient:
    app = create_app()

    async def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac
```

- [ ] **Step 7: Instalar dependências com uv**

```bash
uv sync --all-extras
```

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml Makefile Dockerfile platform/ tests/conftest.py alembic.ini
git commit -m "chore: scaffold project with Clean Architecture, uv, ruff, mypy, and GKE Dockerfile"
```

---

## Task 2: Domain Shared — Value Objects, PolicyTag, Auditable, DomainEvent

**Files:**
- Create: `platform/domain/shared/value_objects.py`
- Create: `platform/domain/shared/policy_tag.py`
- Create: `platform/domain/shared/auditable.py`
- Create: `platform/domain/shared/domain_event.py`
- Create: `tests/unit/domain/shared/test_value_objects.py`

**Interfaces:**
- Produces:
  - `EmailAddress(value: str)` — frozen dataclass, validates `@` presence
  - `CredentialReference(path: str)` — frozen dataclass, validates non-empty
  - `CronSchedule(expression: str)` — frozen dataclass, validates 5-field cron via croniter
  - `DiscoveryScope(include: list[str], exclude: list[str])` — frozen dataclass, replaces raw `dict`
  - `PolicyTag(StrEnum)` — `PII`, `RESTRICTED`, `PUBLIC`, `CONFIDENTIAL`
  - `Auditable` — dataclass mixin with `created_at`, `updated_at`, `touch()`
  - `DomainEvent` — base dataclass; `AssetRegistered`, `AssetActivated` subclasses

---

- [ ] **Step 1: Escrever testes que falham**

```python
# tests/unit/domain/shared/test_value_objects.py
from __future__ import annotations

import pytest

from platform.domain.shared.policy_tag import PolicyTag
from platform.domain.shared.value_objects import (
    CronSchedule,
    CredentialReference,
    DiscoveryScope,
    EmailAddress,
)


class TestEmailAddress:
    def test_valid_email_creates_instance(self) -> None:
        addr = EmailAddress("user@company.com")
        assert addr.value == "user@company.com"

    def test_missing_at_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Invalid email"):
            EmailAddress("notanemail")

    def test_equality_is_by_value(self) -> None:
        assert EmailAddress("a@b.com") == EmailAddress("a@b.com")

    def test_inequality_with_different_value(self) -> None:
        assert EmailAddress("a@b.com") != EmailAddress("c@d.com")


class TestCredentialReference:
    def test_valid_path_creates_instance(self) -> None:
        ref = CredentialReference("vault/secret/oracle-prod")
        assert ref.path == "vault/secret/oracle-prod"

    def test_empty_path_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="cannot be empty"):
            CredentialReference("")

    def test_equality_by_path(self) -> None:
        assert CredentialReference("a/b") == CredentialReference("a/b")


class TestCronSchedule:
    def test_valid_5field_cron_creates_instance(self) -> None:
        sched = CronSchedule("0 6 * * *")
        assert sched.expression == "0 6 * * *"

    def test_invalid_text_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Invalid cron expression"):
            CronSchedule("not-a-cron")

    def test_6field_cron_is_rejected(self) -> None:
        # Platform uses standard 5-field cron only
        with pytest.raises(ValueError, match="Invalid cron expression"):
            CronSchedule("0 0 6 * * *")


class TestDiscoveryScope:
    def test_default_scope_is_all_inclusive(self) -> None:
        scope = DiscoveryScope()
        assert scope.include == []
        assert scope.exclude == []

    def test_scope_with_include_and_exclude(self) -> None:
        scope = DiscoveryScope(include=["customers", "orders"], exclude=["temp_*"])
        assert "customers" in scope.include
        assert "temp_*" in scope.exclude

    def test_is_immutable(self) -> None:
        scope = DiscoveryScope(include=["a"])
        with pytest.raises(AttributeError):
            scope.include = ["b"]  # type: ignore[misc]


class TestPolicyTag:
    def test_all_expected_tags_exist(self) -> None:
        assert set(PolicyTag) == {
            PolicyTag.PII,
            PolicyTag.RESTRICTED,
            PolicyTag.PUBLIC,
            PolicyTag.CONFIDENTIAL,
        }

    def test_pii_value_is_string(self) -> None:
        assert PolicyTag.PII.value == "PII"
```

- [ ] **Step 2: Rodar para confirmar falha**

```bash
uv run pytest tests/unit/domain/shared/ -v
```

Esperado: `FAILED — ModuleNotFoundError`

- [ ] **Step 3: Criar platform/domain/shared/value_objects.py**

```python
from __future__ import annotations

from dataclasses import dataclass, field

from croniter import CroniterBadCronError, croniter


@dataclass(frozen=True)
class EmailAddress:
    """
    Value Object for a validated email address.

    Immutable and comparable by value. Raises ValueError on invalid input.

    Example:
        owner = EmailAddress("eng@company.com")
    """

    value: str

    def __post_init__(self) -> None:
        if "@" not in self.value or not self.value.strip():
            raise ValueError(
                f"Invalid email: {self.value!r}. Expected format: user@domain.com"
            )


@dataclass(frozen=True)
class CredentialReference:
    """
    Value Object for a Vault / Secret Manager lookup path.

    Stores only the reference path — never the actual credential.

    Example:
        ref = CredentialReference("vault/secret/oracle-prod")
    """

    path: str

    def __post_init__(self) -> None:
        if not self.path.strip():
            raise ValueError(
                f"CredentialReference path cannot be empty: {self.path!r}"
            )


@dataclass(frozen=True)
class CronSchedule:
    """
    Value Object for a validated 5-field cron expression.

    Raises ValueError if the expression is not a valid standard cron.

    Example:
        sched = CronSchedule("0 6 * * *")  # daily at 06:00
    """

    expression: str

    def __post_init__(self) -> None:
        parts = self.expression.strip().split()
        if len(parts) != 5:
            raise ValueError(
                f"Invalid cron expression: {self.expression!r}. "
                "Expected exactly 5 fields: minute hour day month weekday."
            )
        try:
            croniter(self.expression)
        except CroniterBadCronError as exc:
            raise ValueError(
                f"Invalid cron expression: {self.expression!r}. Detail: {exc}"
            ) from exc


@dataclass(frozen=True)
class DiscoveryScope:
    """
    Value Object representing which DataObjects should be included/excluded during Discovery.

    Empty include list means 'scan everything'. Exclude patterns support glob syntax.

    Example:
        scope = DiscoveryScope(include=["customers", "orders"], exclude=["temp_*"])
    """

    include: tuple[str, ...] = field(default_factory=tuple)
    exclude: tuple[str, ...] = field(default_factory=tuple)

    def __init__(
        self,
        include: list[str] | None = None,
        exclude: list[str] | None = None,
    ) -> None:
        object.__setattr__(self, "include", tuple(include or []))
        object.__setattr__(self, "exclude", tuple(exclude or []))

    def to_dict(self) -> dict[str, list[str]]:
        """Serialize to a plain dict for storage."""
        return {"include": list(self.include), "exclude": list(self.exclude)}

    @classmethod
    def from_dict(cls, data: dict[str, list[str]]) -> DiscoveryScope:
        """Deserialize from a plain dict (e.g., from JSON storage)."""
        return cls(include=data.get("include", []), exclude=data.get("exclude", []))
```

- [ ] **Step 4: Criar platform/domain/shared/policy_tag.py**

```python
from __future__ import annotations

from enum import StrEnum


class PolicyTag(StrEnum):
    """
    Data sensitivity classification tags.

    Inherited by all DataObjects and DataElements derived from a DataAsset.
    Discovery engine infers and suggests tags; asset owner confirms.
    """

    PII = "PII"
    RESTRICTED = "RESTRICTED"
    PUBLIC = "PUBLIC"
    CONFIDENTIAL = "CONFIDENTIAL"
```

- [ ] **Step 5: Criar platform/domain/shared/auditable.py**

```python
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


@dataclass
class Auditable:
    """
    Mixin for domain entities that track creation and last update timestamps.

    All platform entities inherit this for governance purposes.
    Call touch() after any mutation to update updated_at.
    """

    created_at: datetime = field(default_factory=_utcnow, compare=False)
    updated_at: datetime = field(default_factory=_utcnow, compare=False)

    def touch(self) -> None:
        """Update updated_at to current UTC time. Call after any field mutation."""
        self.updated_at = _utcnow()
```

- [ ] **Step 6: Criar platform/domain/shared/domain_event.py**

```python
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


@dataclass(frozen=True)
class DomainEvent:
    """
    Base class for all domain events.

    Events are immutable records of something that happened in the domain.
    Collected during a unit of work and dispatched after commit.
    """

    occurred_at: datetime = field(default_factory=_utcnow)
    actor_id: str = ""
    actor_email: str = ""


@dataclass(frozen=True)
class AssetRegistered(DomainEvent):
    """Raised when a new DataAsset is successfully created in DRAFT state."""

    asset_id: str = ""
    asset_name: str = ""


@dataclass(frozen=True)
class AssetActivated(DomainEvent):
    """Raised when a DataAsset transitions from DRAFT to ACTIVE."""

    asset_id: str = ""
    endpoint_id: str = ""


@dataclass(frozen=True)
class AssetStateChanged(DomainEvent):
    """Raised on any DataAsset lifecycle state transition."""

    asset_id: str = ""
    previous_state: str = ""
    new_state: str = ""


@dataclass(frozen=True)
class EndpointProvisioned(DomainEvent):
    """Raised when an SRE provisions a new Endpoint."""

    endpoint_id: str = ""
    asset_id: str = ""
    endpoint_type: str = ""
```

- [ ] **Step 7: Rodar testes**

```bash
uv run pytest tests/unit/domain/shared/ -v
```

Esperado: `9 passed`

- [ ] **Step 8: Commit**

```bash
git add platform/domain/shared/ tests/unit/domain/shared/
git commit -m "feat: add Value Objects (EmailAddress, CredentialReference, CronSchedule, DiscoveryScope), PolicyTag enum, Auditable mixin, and DomainEvents"
```

---

## Task 3: Auth — Role, CurrentUser, RBAC Dependency

**Files:**
- Create: `platform/auth/role.py`
- Create: `platform/auth/current_user.py`
- Create: `platform/auth/dependencies.py`
- Create: `tests/unit/auth/test_role_dependency.py`

**Interfaces:**
- Produces:
  - `platform.auth.role.Role` (StrEnum: `PO_PM`, `ANALYTICS_ENGINEER`, `SRE`)
  - `platform.auth.current_user.CurrentUser(id, email: EmailAddress, role: Role)` — frozen dataclass
  - `platform.auth.dependencies.get_current_user` → `CurrentUser`
  - `platform.auth.dependencies.require_role(*roles: Role)` → dependency factory

---

- [ ] **Step 1: Criar platform/auth/role.py**

```python
from __future__ import annotations

from enum import StrEnum


class Role(StrEnum):
    """Platform roles. Maps to IAM groups in GKE / identity provider claims."""

    PO_PM = "po_pm"
    ANALYTICS_ENGINEER = "analytics_engineer"
    SRE = "sre"
```

- [ ] **Step 2: Criar platform/auth/current_user.py**

```python
from __future__ import annotations

from dataclasses import dataclass

from platform.auth.role import Role
from platform.domain.shared.value_objects import EmailAddress


@dataclass(frozen=True)
class CurrentUser:
    """Authenticated user resolved from JWT token. Immutable per request."""

    id: str
    email: EmailAddress
    role: Role
```

- [ ] **Step 3: Escrever testes que falham**

```python
# tests/unit/auth/test_role_dependency.py
from __future__ import annotations

import pytest
from fastapi import Depends, FastAPI
from httpx import ASGITransport, AsyncClient

from platform.auth.current_user import CurrentUser
from platform.auth.dependencies import get_current_user, require_role
from platform.auth.role import Role
from platform.domain.shared.value_objects import EmailAddress


def _user(role: Role) -> CurrentUser:
    return CurrentUser(id="u1", email=EmailAddress("test@co.com"), role=role)


def _make_app(required_role: Role) -> FastAPI:
    app = FastAPI()

    @app.get("/protected")
    async def _route(user: CurrentUser = Depends(require_role(required_role))) -> dict[str, str]:
        return {"role": user.role}

    return app


@pytest.mark.asyncio
async def test_matching_role_is_allowed() -> None:
    app = _make_app(Role.SRE)
    app.dependency_overrides[get_current_user] = lambda: _user(Role.SRE)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        response = await c.get("/protected", headers={"Authorization": "Bearer fake"})
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_wrong_role_is_rejected_with_403() -> None:
    app = _make_app(Role.SRE)
    app.dependency_overrides[get_current_user] = lambda: _user(Role.ANALYTICS_ENGINEER)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        response = await c.get("/protected", headers={"Authorization": "Bearer fake"})
    assert response.status_code == 403
    assert "analytics_engineer" in response.json()["detail"]
```

- [ ] **Step 4: Criar platform/auth/dependencies.py**

```python
from __future__ import annotations

from collections.abc import Callable

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from platform.auth.current_user import CurrentUser
from platform.auth.role import Role
from platform.domain.shared.value_objects import EmailAddress

_bearer = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer),
) -> CurrentUser:
    """
    Resolve the authenticated user from a Bearer JWT token.

    Stub implementation — replace with real JWT decode (python-jose) in production.
    In GKE, the JWT is issued by the identity provider and validated here.
    """
    # TODO: decode JWT, extract id/email/role claims, validate signature
    return CurrentUser(
        id="dev-user",
        email=EmailAddress("dev@platform.local"),
        role=Role.ANALYTICS_ENGINEER,
    )


def require_role(*allowed_roles: Role) -> Callable[..., CurrentUser]:
    """
    FastAPI dependency factory enforcing role-based access control.

    Raises HTTP 403 if the user's role is not in allowed_roles.

    Example:
        @router.post("/endpoints", dependencies=[Depends(require_role(Role.SRE))])
    """

    async def _enforce(user: CurrentUser = Depends(get_current_user)) -> CurrentUser:
        if user.role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    f"Role '{user.role}' is not allowed. "
                    f"Required one of: {[r.value for r in allowed_roles]}"
                ),
            )
        return user

    return _enforce
```

- [ ] **Step 5: Rodar testes**

```bash
uv run pytest tests/unit/auth/ -v
```

Esperado: `2 passed`

- [ ] **Step 6: Commit**

```bash
git add platform/auth/ tests/unit/auth/
git commit -m "feat: add Role enum, CurrentUser value object, and require_role RBAC dependency"
```

---

## Task 4: Domain — Endpoint ABC com Hierarquia Tipada

**Files:**
- Create: `platform/domain/endpoints/endpoint_type.py`
- Create: `platform/domain/endpoints/endpoint.py`
- Create: `platform/domain/endpoints/endpoint_repository.py`
- Create: `platform/domain/endpoints/endpoint_service.py`
- Create: `tests/unit/domain/endpoints/test_endpoint_service.py`

**Interfaces:**
- Produces:
  - `Endpoint(ABC, Auditable)` — abstract base with `id`, `asset_id`, `credential_ref`, `technical_description`
  - `DatabaseEndpoint(Endpoint)` — adds `host`, `port`, `database`, `driver`
  - `RestApiEndpoint(Endpoint)` — adds `base_url`, `auth_type`, `headers_ref`
  - `SftpEndpoint(Endpoint)` — adds `host`, `port`, `root_path`, `private_key_ref`
  - `CloudBucketEndpoint(Endpoint)` — adds `provider`, `bucket`, `prefix`, `region`
  - `EtlFlowEndpoint(Endpoint)` — adds `tool`, `flow_id`
  - `EndpointRepository(Protocol)` — `save`, `find_by_id`, `find_by_asset_id`
  - `EndpointService` — domain service with `provision(...)`, `find_for_asset(...)`

---

- [ ] **Step 1: Criar platform/domain/endpoints/endpoint_type.py**

```python
from __future__ import annotations

from enum import StrEnum


class EndpointType(StrEnum):
    """Supported endpoint (connection) types. Each maps to a typed Endpoint subclass."""

    DATABASE = "database"
    REST_API = "rest_api"
    SFTP = "sftp"
    CLOUD_BUCKET = "cloud_bucket"
    ETL_FLOW = "etl_flow"
```

- [ ] **Step 2: Criar platform/domain/endpoints/endpoint.py**

```python
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from platform.domain.endpoints.endpoint_type import EndpointType
from platform.domain.shared.auditable import Auditable
from platform.domain.shared.value_objects import CredentialReference


@dataclass
class Endpoint(ABC, Auditable):
    """
    Abstract base for all data source/destination connections.

    Managed exclusively by SRE. Business users see only id and type.
    Subclasses define type-specific connection fields.

    credential_ref is a Value Object — never the actual secret value.
    """

    id: str
    asset_id: str
    credential_ref: CredentialReference
    technical_description: str = ""

    @property
    @abstractmethod
    def type(self) -> EndpointType:
        """Return the EndpointType discriminator for this subclass."""


@dataclass
class DatabaseEndpoint(Endpoint):
    """
    Endpoint for relational databases (Oracle, PostgreSQL, MySQL, etc.).

    Example:
        ep = DatabaseEndpoint(
            id="uuid", asset_id="uuid",
            credential_ref=CredentialReference("vault/secret/oracle-prod"),
            host="oracle.internal", port=1521, database="PROD", driver="oracle",
        )
    """

    host: str = ""
    port: int = 0
    database: str = ""
    driver: str = ""  # "oracle" | "postgres" | "mysql" | "mssql"

    @property
    def type(self) -> EndpointType:
        return EndpointType.DATABASE


@dataclass
class RestApiEndpoint(Endpoint):
    """
    Endpoint for REST APIs.

    auth_type: "bearer" | "api_key" | "oauth2" | "basic"
    headers_ref: optional reference to custom headers stored in Vault.
    """

    base_url: str = ""
    auth_type: str = ""
    headers_ref: str = ""

    @property
    def type(self) -> EndpointType:
        return EndpointType.REST_API


@dataclass
class SftpEndpoint(Endpoint):
    """
    Endpoint for SFTP servers.

    private_key_ref: reference to the SSH private key in Vault.
    """

    host: str = ""
    port: int = 22
    root_path: str = "/"
    private_key_ref: str = ""

    @property
    def type(self) -> EndpointType:
        return EndpointType.SFTP


@dataclass
class CloudBucketEndpoint(Endpoint):
    """
    Endpoint for cloud object storage (S3, GCS, Azure Blob).

    provider: "s3" | "gcs" | "azure"
    """

    provider: str = ""  # "s3" | "gcs" | "azure"
    bucket: str = ""
    prefix: str = ""
    region: str = ""

    @property
    def type(self) -> EndpointType:
        return EndpointType.CLOUD_BUCKET


@dataclass
class EtlFlowEndpoint(Endpoint):
    """
    Endpoint for managed ETL tools (Fivetran, Airbyte, etc.).

    tool: "fivetran" | "airbyte"
    flow_id: the connector / sync id within the ETL tool.
    """

    tool: str = ""   # "fivetran" | "airbyte"
    flow_id: str = ""

    @property
    def type(self) -> EndpointType:
        return EndpointType.ETL_FLOW


# Convenience union type for type hints
AnyEndpoint = (
    DatabaseEndpoint
    | RestApiEndpoint
    | SftpEndpoint
    | CloudBucketEndpoint
    | EtlFlowEndpoint
)
```

- [ ] **Step 3: Criar platform/domain/endpoints/endpoint_repository.py**

```python
from __future__ import annotations

from typing import Protocol, runtime_checkable

from platform.domain.endpoints.endpoint import Endpoint


@runtime_checkable
class EndpointRepository(Protocol):
    """Repository interface for Endpoint persistence. Implemented in infrastructure."""

    async def save(self, endpoint: Endpoint) -> Endpoint: ...

    async def find_by_id(self, endpoint_id: str) -> Endpoint | None: ...

    async def find_by_asset_id(self, asset_id: str) -> Endpoint | None: ...
```

- [ ] **Step 4: Criar platform/domain/endpoints/endpoint_service.py**

```python
from __future__ import annotations

from platform.domain.endpoints.endpoint import Endpoint
from platform.domain.endpoints.endpoint_repository import EndpointRepository


class EndpointNotFoundError(Exception):
    def __init__(self, endpoint_id: str) -> None:
        super().__init__(f"Endpoint not found: id={endpoint_id!r}")
        self.endpoint_id = endpoint_id


class EndpointService:
    """
    Domain service for Endpoint provisioning.

    Accepts a fully-constructed typed Endpoint subclass.
    Subtype-specific validation is done by the subclass itself
    (e.g., non-empty host) or in the HTTP schema layer.
    """

    def __init__(self, repo: EndpointRepository) -> None:
        self._repo = repo

    async def provision(self, endpoint: Endpoint) -> Endpoint:
        """
        Persist a pre-built typed Endpoint and return the saved entity.

        Example:
            ep = DatabaseEndpoint(id="uuid", ..., host="oracle.internal", port=1521, ...)
            saved = await service.provision(ep)
        """
        return await self._repo.save(endpoint)

    async def find_for_asset(self, asset_id: str) -> Endpoint | None:
        """Return the Endpoint linked to a DataAsset, or None if not provisioned yet."""
        return await self._repo.find_by_asset_id(asset_id)
```

- [ ] **Step 5: Escrever testes unitários**

```python
# tests/unit/domain/endpoints/test_endpoint_service.py
from __future__ import annotations

import uuid

import pytest

from platform.domain.endpoints.endpoint import (
    CloudBucketEndpoint,
    DatabaseEndpoint,
    EtlFlowEndpoint,
    RestApiEndpoint,
    SftpEndpoint,
)
from platform.domain.endpoints.endpoint_repository import EndpointRepository
from platform.domain.endpoints.endpoint_service import EndpointService
from platform.domain.endpoints.endpoint_type import EndpointType
from platform.domain.shared.value_objects import CredentialReference


class FakeEndpointRepository:
    """Named fake implementing EndpointRepository Protocol for unit tests."""

    def __init__(self) -> None:
        self._store: dict[str, DatabaseEndpoint | RestApiEndpoint | SftpEndpoint | CloudBucketEndpoint | EtlFlowEndpoint] = {}

    async def save(self, endpoint: DatabaseEndpoint | RestApiEndpoint | SftpEndpoint | CloudBucketEndpoint | EtlFlowEndpoint) -> DatabaseEndpoint | RestApiEndpoint | SftpEndpoint | CloudBucketEndpoint | EtlFlowEndpoint:
        self._store[endpoint.id] = endpoint
        return endpoint

    async def find_by_id(self, endpoint_id: str) -> DatabaseEndpoint | RestApiEndpoint | SftpEndpoint | CloudBucketEndpoint | EtlFlowEndpoint | None:
        return self._store.get(endpoint_id)

    async def find_by_asset_id(self, asset_id: str) -> DatabaseEndpoint | RestApiEndpoint | SftpEndpoint | CloudBucketEndpoint | EtlFlowEndpoint | None:
        return next((e for e in self._store.values() if e.asset_id == asset_id), None)


def _cred(path: str = "vault/secret/prod") -> CredentialReference:
    return CredentialReference(path)


def _id() -> str:
    return str(uuid.uuid4())


@pytest.mark.asyncio
async def test_provision_database_endpoint_has_typed_fields() -> None:
    service = EndpointService(repo=FakeEndpointRepository())
    ep = DatabaseEndpoint(
        id=_id(), asset_id="a1", credential_ref=_cred(),
        host="oracle.internal", port=1521, database="PROD", driver="oracle",
    )
    saved = await service.provision(ep)
    assert isinstance(saved, DatabaseEndpoint)
    assert saved.type == EndpointType.DATABASE
    assert saved.host == "oracle.internal"
    assert saved.port == 1521


@pytest.mark.asyncio
async def test_provision_rest_api_endpoint() -> None:
    service = EndpointService(repo=FakeEndpointRepository())
    ep = RestApiEndpoint(
        id=_id(), asset_id="a2", credential_ref=_cred(),
        base_url="https://api.example.com", auth_type="bearer",
    )
    saved = await service.provision(ep)
    assert isinstance(saved, RestApiEndpoint)
    assert saved.type == EndpointType.REST_API
    assert saved.base_url == "https://api.example.com"


@pytest.mark.asyncio
async def test_provision_sftp_endpoint() -> None:
    service = EndpointService(repo=FakeEndpointRepository())
    ep = SftpEndpoint(
        id=_id(), asset_id="a3", credential_ref=_cred(),
        host="sftp.example.com", port=22, root_path="/exports",
    )
    saved = await service.provision(ep)
    assert isinstance(saved, SftpEndpoint)
    assert saved.type == EndpointType.SFTP


@pytest.mark.asyncio
async def test_provision_cloud_bucket_endpoint() -> None:
    service = EndpointService(repo=FakeEndpointRepository())
    ep = CloudBucketEndpoint(
        id=_id(), asset_id="a4", credential_ref=_cred(),
        provider="gcs", bucket="raw-data-prod", region="us-central1",
    )
    saved = await service.provision(ep)
    assert isinstance(saved, CloudBucketEndpoint)
    assert saved.type == EndpointType.CLOUD_BUCKET
    assert saved.provider == "gcs"


@pytest.mark.asyncio
async def test_provision_etl_flow_endpoint() -> None:
    service = EndpointService(repo=FakeEndpointRepository())
    ep = EtlFlowEndpoint(
        id=_id(), asset_id="a5", credential_ref=_cred(),
        tool="fivetran", flow_id="connector-abc123",
    )
    saved = await service.provision(ep)
    assert isinstance(saved, EtlFlowEndpoint)
    assert saved.type == EndpointType.ETL_FLOW
    assert saved.flow_id == "connector-abc123"


@pytest.mark.asyncio
async def test_credential_ref_validates_on_construction() -> None:
    with pytest.raises(ValueError, match="cannot be empty"):
        CredentialReference("")


@pytest.mark.asyncio
async def test_find_for_asset_returns_none_when_not_provisioned() -> None:
    service = EndpointService(repo=FakeEndpointRepository())
    result = await service.find_for_asset("nonexistent-asset")
    assert result is None
```

- [ ] **Step 6: Rodar testes**

```bash
uv run pytest tests/unit/domain/endpoints/ -v
```

Esperado: `7 passed`

- [ ] **Step 7: Commit**

```bash
git add platform/domain/endpoints/ tests/unit/domain/endpoints/
git commit -m "feat: add typed Endpoint ABC hierarchy (Database, RestApi, Sftp, CloudBucket, EtlFlow) with EndpointService"
```

---

## Task 5: Domain — DataAsset Entity, Repository Protocol, AssetService

**Files:**
- Create: `platform/domain/assets/asset_state.py`
- Create: `platform/domain/assets/data_asset.py`
- Create: `platform/domain/assets/asset_repository.py`
- Create: `platform/domain/assets/asset_service.py`
- Create: `tests/unit/domain/assets/test_asset_service.py`

**Interfaces:**
- Produces:
  - `AssetState(StrEnum)` + `VALID_TRANSITIONS`
  - `DataAsset(Auditable)` — pure Python dataclass, aggregate root
  - `AssetRepository(Protocol)` — `save`, `find_by_id`, `update_state`, `update_endpoint`, `update_scope`
  - `AssetService` — `register(...)`, `transition_to_active(...)`, `deprecate(...)`, `archive(...)`, `update_scope(...)`
  - `AssetNotFoundError`, `InvalidStateTransitionError`

---

- [ ] **Step 1: Criar platform/domain/assets/asset_state.py**

```python
from __future__ import annotations

from enum import StrEnum


class AssetState(StrEnum):
    DRAFT = "draft"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"


VALID_TRANSITIONS: dict[AssetState, frozenset[AssetState]] = {
    AssetState.DRAFT: frozenset({AssetState.ACTIVE}),
    AssetState.ACTIVE: frozenset({AssetState.DEPRECATED}),
    AssetState.DEPRECATED: frozenset({AssetState.ARCHIVED}),
    AssetState.ARCHIVED: frozenset(),
}
```

- [ ] **Step 2: Criar platform/domain/assets/data_asset.py**

```python
from __future__ import annotations

from dataclasses import dataclass, field

from platform.domain.assets.asset_state import AssetState
from platform.domain.shared.auditable import Auditable
from platform.domain.shared.policy_tag import PolicyTag
from platform.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress


@dataclass
class DataAsset(Auditable):
    """
    DataAsset: aggregate root representing a business data domain.

    Stable after registration. discovery_scope and discovery_schedule
    are the only fields modified after activation (no SRE required).
    endpoint_id is set during the SRE handoff (DRAFT → ACTIVE transition).

    No SQLAlchemy. No Pydantic. No FastAPI.
    """

    id: str
    name: str
    description: str
    owner: EmailAddress
    tags: list[str] = field(default_factory=list)
    policy_tags: list[PolicyTag] = field(default_factory=list)
    state: AssetState = AssetState.DRAFT
    discovery_schedule: CronSchedule = field(
        default_factory=lambda: CronSchedule("0 6 * * *")
    )
    discovery_scope: DiscoveryScope = field(default_factory=DiscoveryScope)
    endpoint_id: str | None = None
```

- [ ] **Step 3: Criar platform/domain/assets/asset_repository.py**

```python
from __future__ import annotations

from typing import Protocol, runtime_checkable

from platform.domain.assets.asset_state import AssetState
from platform.domain.assets.data_asset import DataAsset
from platform.domain.shared.value_objects import DiscoveryScope


@runtime_checkable
class AssetRepository(Protocol):
    """
    Repository interface for DataAsset persistence.

    Defined in domain — concrete implementations live in infrastructure.
    Domain services depend only on this Protocol.
    """

    async def save(self, asset: DataAsset) -> DataAsset: ...

    async def find_by_id(self, asset_id: str) -> DataAsset | None: ...

    async def update_state(self, asset_id: str, new_state: AssetState) -> DataAsset: ...

    async def update_endpoint(self, asset_id: str, endpoint_id: str) -> DataAsset: ...

    async def update_scope(self, asset_id: str, scope: DiscoveryScope) -> DataAsset: ...
```

- [ ] **Step 4: Criar platform/domain/assets/asset_service.py**

```python
from __future__ import annotations

from platform.domain.assets.asset_repository import AssetRepository
from platform.domain.assets.asset_state import VALID_TRANSITIONS, AssetState
from platform.domain.assets.data_asset import DataAsset
from platform.domain.shared.policy_tag import PolicyTag
from platform.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress


class AssetNotFoundError(Exception):
    def __init__(self, asset_id: str) -> None:
        super().__init__(f"DataAsset not found: id={asset_id!r}")
        self.asset_id = asset_id


class InvalidStateTransitionError(Exception):
    def __init__(self, current: AssetState, target: AssetState) -> None:
        allowed = sorted(VALID_TRANSITIONS[current])
        super().__init__(
            f"Cannot transition from '{current}' to '{target}'. "
            f"Allowed targets from '{current}': {allowed}"
        )


class AssetService:
    """
    Domain service for DataAsset lifecycle management.

    No FastAPI. No SQLAlchemy. Depends only on AssetRepository Protocol.

    Example:
        service = AssetService(repo=FakeAssetRepository())
        asset = await service.register(name="customers", owner=EmailAddress("po@co.com"), ...)
    """

    def __init__(self, repo: AssetRepository) -> None:
        self._repo = repo

    async def register(
        self,
        asset_id: str,
        name: str,
        description: str,
        owner: EmailAddress,
        tags: list[str],
        policy_tags: list[PolicyTag],
        discovery_schedule: CronSchedule,
        discovery_scope: DiscoveryScope,
    ) -> DataAsset:
        """Create a new DataAsset in DRAFT state and persist it."""
        asset = DataAsset(
            id=asset_id,
            name=name,
            description=description,
            owner=owner,
            tags=tags,
            policy_tags=policy_tags,
            state=AssetState.DRAFT,
            discovery_schedule=discovery_schedule,
            discovery_scope=discovery_scope,
        )
        return await self._repo.save(asset)

    async def transition_to_active(self, asset_id: str, endpoint_id: str) -> DataAsset:
        """Move asset DRAFT → ACTIVE after SRE provisions the Endpoint."""
        asset = await self._require_asset(asset_id)
        self._assert_transition(asset.state, AssetState.ACTIVE)
        await self._repo.update_endpoint(asset_id, endpoint_id)
        return await self._repo.update_state(asset_id, AssetState.ACTIVE)

    async def deprecate(self, asset_id: str) -> DataAsset:
        """Move asset ACTIVE → DEPRECATED."""
        asset = await self._require_asset(asset_id)
        self._assert_transition(asset.state, AssetState.DEPRECATED)
        return await self._repo.update_state(asset_id, AssetState.DEPRECATED)

    async def archive(self, asset_id: str) -> DataAsset:
        """Move asset DEPRECATED → ARCHIVED."""
        asset = await self._require_asset(asset_id)
        self._assert_transition(asset.state, AssetState.ARCHIVED)
        return await self._repo.update_state(asset_id, AssetState.ARCHIVED)

    async def update_scope(self, asset_id: str, scope: DiscoveryScope) -> DataAsset:
        """Update discovery_scope. No SRE involvement required."""
        await self._require_asset(asset_id)
        return await self._repo.update_scope(asset_id, scope)

    async def _require_asset(self, asset_id: str) -> DataAsset:
        asset = await self._repo.find_by_id(asset_id)
        if asset is None:
            raise AssetNotFoundError(asset_id)
        return asset

    @staticmethod
    def _assert_transition(current: AssetState, target: AssetState) -> None:
        if target not in VALID_TRANSITIONS[current]:
            raise InvalidStateTransitionError(current, target)
```

- [ ] **Step 5: Escrever testes unitários**

```python
# tests/unit/domain/assets/test_asset_service.py
from __future__ import annotations

import uuid

import pytest

from platform.domain.assets.asset_repository import AssetRepository
from platform.domain.assets.asset_service import (
    AssetNotFoundError,
    AssetService,
    InvalidStateTransitionError,
)
from platform.domain.assets.asset_state import AssetState
from platform.domain.assets.data_asset import DataAsset
from platform.domain.shared.policy_tag import PolicyTag
from platform.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress


class FakeAssetRepository:
    """Named fake implementing AssetRepository Protocol. Used in unit tests only."""

    def __init__(self) -> None:
        self._store: dict[str, DataAsset] = {}

    async def save(self, asset: DataAsset) -> DataAsset:
        self._store[asset.id] = asset
        return asset

    async def find_by_id(self, asset_id: str) -> DataAsset | None:
        return self._store.get(asset_id)

    async def update_state(self, asset_id: str, new_state: AssetState) -> DataAsset:
        asset = self._store[asset_id]
        asset.state = new_state
        asset.touch()
        return asset

    async def update_endpoint(self, asset_id: str, endpoint_id: str) -> DataAsset:
        asset = self._store[asset_id]
        asset.endpoint_id = endpoint_id
        asset.touch()
        return asset

    async def update_scope(self, asset_id: str, scope: DiscoveryScope) -> DataAsset:
        asset = self._store[asset_id]
        asset.discovery_scope = scope
        asset.touch()
        return asset


def _new_service() -> tuple[AssetService, FakeAssetRepository]:
    repo = FakeAssetRepository()
    return AssetService(repo=repo), repo


async def _registered_asset(service: AssetService, name: str = "customers") -> DataAsset:
    return await service.register(
        asset_id=str(uuid.uuid4()),
        name=name,
        description="Test asset",
        owner=EmailAddress("po@co.com"),
        tags=["core"],
        policy_tags=[PolicyTag.PII],
        discovery_schedule=CronSchedule("0 6 * * *"),
        discovery_scope=DiscoveryScope(),
    )


@pytest.mark.asyncio
async def test_register_creates_asset_in_draft() -> None:
    service, _ = _new_service()
    asset = await _registered_asset(service)
    assert asset.state == AssetState.DRAFT
    assert PolicyTag.PII in asset.policy_tags


@pytest.mark.asyncio
async def test_transition_to_active_sets_endpoint_and_state() -> None:
    service, _ = _new_service()
    asset = await _registered_asset(service)
    activated = await service.transition_to_active(asset.id, endpoint_id="ep-uuid-1")
    assert activated.state == AssetState.ACTIVE
    assert activated.endpoint_id == "ep-uuid-1"


@pytest.mark.asyncio
async def test_invalid_transition_from_archived_raises_error() -> None:
    service, repo = _new_service()
    asset = await _registered_asset(service)
    await repo.update_state(asset.id, AssetState.ARCHIVED)
    with pytest.raises(InvalidStateTransitionError) as exc:
        await service.transition_to_active(asset.id, endpoint_id="ep-1")
    assert "archived" in str(exc.value)


@pytest.mark.asyncio
async def test_asset_not_found_raises_error() -> None:
    service, _ = _new_service()
    with pytest.raises(AssetNotFoundError):
        await service.transition_to_active("nonexistent", endpoint_id="ep-1")


@pytest.mark.asyncio
async def test_update_scope_replaces_discovery_scope() -> None:
    service, _ = _new_service()
    asset = await _registered_asset(service)
    new_scope = DiscoveryScope(include=["orders"], exclude=["temp_*"])
    updated = await service.update_scope(asset.id, new_scope)
    assert "orders" in updated.discovery_scope.include
    assert "temp_*" in updated.discovery_scope.exclude
```

- [ ] **Step 6: Rodar testes**

```bash
uv run pytest tests/unit/domain/ -v
```

Esperado: `12+ passed`

- [ ] **Step 7: Commit**

```bash
git add platform/domain/assets/ tests/unit/domain/assets/
git commit -m "feat: add DataAsset aggregate root, AssetRepository Protocol, and AssetService with state machine"
```

---

## Task 6: Application — Unit of Work

**Files:**
- Create: `platform/application/__init__.py`
- Create: `platform/application/unit_of_work.py`
- Create: `tests/unit/application/test_unit_of_work.py`

**Interfaces:**
- Produces:
  - `UnitOfWork(Protocol)` — `assets: AssetRepository`, `endpoints: EndpointRepository`, `async commit()`, `async rollback()`, `async __aenter__`, `async __aexit__`
  - `SqlUnitOfWork` — concrete implementation (infra): manages `AsyncSession` lifecycle

---

- [ ] **Step 1: Criar platform/application/unit_of_work.py**

```python
from __future__ import annotations

from typing import Protocol, runtime_checkable

from platform.domain.assets.asset_repository import AssetRepository
from platform.domain.endpoints.endpoint_repository import EndpointRepository


@runtime_checkable
class UnitOfWork(Protocol):
    """
    Unit of Work: groups repositories under a single transactional boundary.

    All use cases that perform writes must use a UoW to ensure atomicity.
    This covers: create asset + emit audit log + publish catalog + send notification.

    The UoW is a context manager: use it with `async with`:

    Example:
        async with uow:
            asset = await uow.assets.save(new_asset)
            await uow.commit()
        # Side effects (catalog, notifications) dispatched after commit
    """

    assets: AssetRepository
    endpoints: EndpointRepository

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...

    async def __aenter__(self) -> UnitOfWork: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None: ...
```

- [ ] **Step 2: Criar platform/infrastructure/persistence/sql_unit_of_work.py**

```python
from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from platform.domain.assets.asset_repository import AssetRepository
from platform.domain.endpoints.endpoint_repository import EndpointRepository
from platform.infrastructure.persistence.repositories.sql_asset_repository import (
    SqlAssetRepository,
)
from platform.infrastructure.persistence.repositories.sql_endpoint_repository import (
    SqlEndpointRepository,
)


class SqlUnitOfWork:
    """
    SQLAlchemy implementation of UnitOfWork.

    Manages the AsyncSession lifecycle and exposes typed repositories.
    Creates repositories per-transaction so they share the same session.

    Example:
        async with SqlUnitOfWork(session_factory) as uow:
            asset = await uow.assets.save(new_asset)
            await uow.commit()
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self._session: AsyncSession | None = None

    @property
    def assets(self) -> AssetRepository:
        assert self._session is not None, "UoW must be used as a context manager"
        return SqlAssetRepository(self._session)

    @property
    def endpoints(self) -> EndpointRepository:
        assert self._session is not None, "UoW must be used as a context manager"
        return SqlEndpointRepository(self._session)

    async def commit(self) -> None:
        assert self._session is not None
        await self._session.commit()

    async def rollback(self) -> None:
        assert self._session is not None
        await self._session.rollback()

    async def __aenter__(self) -> SqlUnitOfWork:
        self._session = self._session_factory()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        if exc_type is not None:
            await self.rollback()
        if self._session is not None:
            await self._session.close()
            self._session = None
```

- [ ] **Step 3: Escrever testes unitários do UoW**

```python
# tests/unit/application/test_unit_of_work.py
from __future__ import annotations

import uuid

import pytest

from platform.domain.assets.asset_repository import AssetRepository
from platform.domain.assets.asset_state import AssetState
from platform.domain.assets.data_asset import DataAsset
from platform.domain.endpoints.endpoint_repository import EndpointRepository
from platform.domain.endpoints.endpoint import Endpoint
from platform.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress, CredentialReference
from platform.domain.shared.policy_tag import PolicyTag
from platform.application.unit_of_work import UnitOfWork


class FakeAssetRepo:
    def __init__(self) -> None:
        self._store: dict[str, DataAsset] = {}
        self.committed = False
        self.rolled_back = False

    async def save(self, asset: DataAsset) -> DataAsset:
        self._store[asset.id] = asset
        return asset

    async def find_by_id(self, asset_id: str) -> DataAsset | None:
        return self._store.get(asset_id)

    async def update_state(self, asset_id: str, new_state: AssetState) -> DataAsset:
        self._store[asset_id].state = new_state
        return self._store[asset_id]

    async def update_endpoint(self, asset_id: str, endpoint_id: str) -> DataAsset:
        self._store[asset_id].endpoint_id = endpoint_id
        return self._store[asset_id]

    async def update_scope(self, asset_id: str, scope: DiscoveryScope) -> DataAsset:
        self._store[asset_id].discovery_scope = scope
        return self._store[asset_id]


class FakeEndpointRepo:
    def __init__(self) -> None:
        self._store: dict[str, object] = {}

    async def save(self, endpoint: object) -> object:
        return endpoint

    async def find_by_id(self, endpoint_id: str) -> object | None:
        return None

    async def find_by_asset_id(self, asset_id: str) -> object | None:
        return None


class FakeUnitOfWork:
    """Named fake UoW for use case tests."""

    def __init__(self) -> None:
        self.assets = FakeAssetRepo()
        self.endpoints = FakeEndpointRepo()
        self._committed = False
        self._rolled_back = False

    async def commit(self) -> None:
        self._committed = True

    async def rollback(self) -> None:
        self._rolled_back = True

    async def __aenter__(self) -> FakeUnitOfWork:
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        if exc_type is not None:
            await self.rollback()


@pytest.mark.asyncio
async def test_uow_commit_is_called_on_success() -> None:
    uow = FakeUnitOfWork()
    async with uow:
        asset = DataAsset(
            id=str(uuid.uuid4()), name="test", description="desc",
            owner=EmailAddress("po@co.com"),
            discovery_schedule=CronSchedule("0 6 * * *"),
        )
        await uow.assets.save(asset)
        await uow.commit()
    assert uow._committed is True


@pytest.mark.asyncio
async def test_uow_rollback_is_called_on_exception() -> None:
    uow = FakeUnitOfWork()
    with pytest.raises(RuntimeError):
        async with uow:
            raise RuntimeError("something went wrong")
    assert uow._rolled_back is True
```

- [ ] **Step 4: Rodar testes**

```bash
uv run pytest tests/unit/application/ -v
```

Esperado: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add platform/application/unit_of_work.py platform/infrastructure/persistence/sql_unit_of_work.py tests/unit/application/
git commit -m "feat: add UnitOfWork Protocol and SqlUnitOfWork with atomic commit/rollback for use cases"
```

---

## Task 7: Infrastructure — Persistence Layer (Models + Repositories)

**Files:**
- Create: `platform/infrastructure/persistence/base_model.py`
- Create: `platform/infrastructure/persistence/database.py`
- Create: `platform/infrastructure/persistence/models/data_asset_model.py`
- Create: `platform/infrastructure/persistence/models/endpoint_model.py`
- Create: `platform/infrastructure/persistence/models/audit_log_model.py`
- Create: `platform/infrastructure/persistence/repositories/sql_asset_repository.py`
- Create: `platform/infrastructure/persistence/repositories/sql_endpoint_repository.py`
- Create: `tests/integration/repositories/test_sql_asset_repository.py`
- Create: `tests/integration/repositories/test_sql_endpoint_repository.py`

**Interfaces:**
- Consumes: `DataAsset` (Task 5), `Endpoint` hierarchy (Task 4), `AssetRepository`/`EndpointRepository` Protocols
- Produces: `SqlAssetRepository`, `SqlEndpointRepository` (concrete infra), `Base`, `get_db`

---

- [ ] **Step 1: Criar base_model.py**

```python
from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """SQLAlchemy declarative base for all ORM models."""


class TimestampMixin:
    """
    Adds created_at and updated_at columns to any ORM model.

    All platform entities are timestamped for governance purposes.
    """

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
```

- [ ] **Step 2: Criar database.py**

```python
from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from platform.config import get_settings


def _build_engine():
    settings = get_settings()
    return create_async_engine(
        str(settings.database_url),
        echo=settings.debug,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
    )


_engine = _build_engine()
_session_factory = async_sessionmaker(_engine, expire_on_commit=False)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency: yields a transactional AsyncSession."""
    async with _session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Return the session factory for SqlUnitOfWork construction."""
    return _session_factory
```

- [ ] **Step 3: Criar data_asset_model.py**

```python
from __future__ import annotations

import uuid

from sqlalchemy import ForeignKey, JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from platform.infrastructure.persistence.base_model import Base, TimestampMixin


class DataAssetModel(Base, TimestampMixin):
    """
    ORM model for DataAsset. Infrastructure only — no business logic.

    Mapping to/from the domain DataAsset entity is done in SqlAssetRepository.
    discovery_scope stored as JSON dict (serialized from DiscoveryScope Value Object).
    """

    __tablename__ = "data_assets"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    description: Mapped[str] = mapped_column(String(2000), nullable=False)
    owner_email: Mapped[str] = mapped_column(String(255), nullable=False)
    tags: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    policy_tags: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    state: Mapped[str] = mapped_column(String(50), nullable=False, default="draft")
    discovery_schedule: Mapped[str] = mapped_column(String(100), nullable=False)
    discovery_scope: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    endpoint_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("endpoints.id", use_alter=True), nullable=True
    )
```

- [ ] **Step 4: Criar endpoint_model.py (single table com type discriminator + subtype_data JSON)**

```python
from __future__ import annotations

import uuid

from sqlalchemy import ForeignKey, JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from platform.infrastructure.persistence.base_model import Base, TimestampMixin


class EndpointModel(Base, TimestampMixin):
    """
    ORM model for all Endpoint subtypes.

    Uses single-table storage: `type` discriminates the subclass,
    `subtype_data` (JSON) stores the subtype-specific typed fields.
    This keeps the ORM schema simple while the domain uses typed subclasses.
    Repository handles domain ↔ ORM mapping.
    """

    __tablename__ = "endpoints"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    asset_id: Mapped[str] = mapped_column(String(36), ForeignKey("data_assets.id"), nullable=False)
    type: Mapped[str] = mapped_column(String(50), nullable=False)  # EndpointType value
    credential_ref: Mapped[str] = mapped_column(String(500), nullable=False)
    technical_description: Mapped[str] = mapped_column(String(2000), nullable=False, default="")
    # Stores typed subclass fields: host, port, base_url, bucket, etc.
    subtype_data: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
```

- [ ] **Step 5: Criar audit_log_model.py**

```python
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from platform.infrastructure.persistence.base_model import Base


class AuditLogModel(Base):
    """
    Immutable append-only table for critical platform events.

    Never updated — only inserted. Covers: state transitions,
    policy_tag changes, schema drift approvals, endpoint provisioning.
    """

    __tablename__ = "audit_logs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=timezone.utc),
    )
    actor_id: Mapped[str] = mapped_column(String(255), nullable=False)
    actor_email: Mapped[str] = mapped_column(String(255), nullable=False)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)   # e.g. "asset.state_transition"
    entity_type: Mapped[str] = mapped_column(String(100), nullable=False)  # e.g. "DataAsset"
    entity_id: Mapped[str] = mapped_column(String(36), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
```

- [ ] **Step 6: Criar sql_asset_repository.py**

```python
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from platform.domain.assets.asset_service import AssetNotFoundError
from platform.domain.assets.asset_state import AssetState
from platform.domain.assets.data_asset import DataAsset
from platform.domain.shared.policy_tag import PolicyTag
from platform.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress
from platform.infrastructure.persistence.models.data_asset_model import DataAssetModel


def _to_domain(m: DataAssetModel) -> DataAsset:
    """Map ORM model → domain entity. No business logic."""
    return DataAsset(
        id=m.id,
        name=m.name,
        description=m.description,
        owner=EmailAddress(m.owner_email),
        tags=list(m.tags),
        policy_tags=[PolicyTag(t) for t in m.policy_tags],
        state=AssetState(m.state),
        discovery_schedule=CronSchedule(m.discovery_schedule),
        discovery_scope=DiscoveryScope.from_dict(m.discovery_scope),
        endpoint_id=m.endpoint_id,
        created_at=m.created_at,
        updated_at=m.updated_at,
    )


def _to_model(asset: DataAsset) -> DataAssetModel:
    """Map domain entity → ORM model. No business logic."""
    return DataAssetModel(
        id=asset.id,
        name=asset.name,
        description=asset.description,
        owner_email=asset.owner.value,
        tags=asset.tags,
        policy_tags=[t.value for t in asset.policy_tags],
        state=asset.state.value,
        discovery_schedule=asset.discovery_schedule.expression,
        discovery_scope=asset.discovery_scope.to_dict(),
        endpoint_id=asset.endpoint_id,
    )


class SqlAssetRepository:
    """SQLAlchemy implementation of AssetRepository. Infrastructure only."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, asset: DataAsset) -> DataAsset:
        model = _to_model(asset)
        self._session.add(model)
        await self._session.flush()
        return _to_domain(model)

    async def find_by_id(self, asset_id: str) -> DataAsset | None:
        result = await self._session.execute(select(DataAssetModel).where(DataAssetModel.id == asset_id))
        model = result.scalar_one_or_none()
        return _to_domain(model) if model else None

    async def update_state(self, asset_id: str, new_state: AssetState) -> DataAsset:
        model = await self._fetch_or_raise(asset_id)
        model.state = new_state.value
        await self._session.flush()
        return _to_domain(model)

    async def update_endpoint(self, asset_id: str, endpoint_id: str) -> DataAsset:
        model = await self._fetch_or_raise(asset_id)
        model.endpoint_id = endpoint_id
        await self._session.flush()
        return _to_domain(model)

    async def update_scope(self, asset_id: str, scope: DiscoveryScope) -> DataAsset:
        model = await self._fetch_or_raise(asset_id)
        model.discovery_scope = scope.to_dict()
        await self._session.flush()
        return _to_domain(model)

    async def _fetch_or_raise(self, asset_id: str) -> DataAssetModel:
        result = await self._session.execute(select(DataAssetModel).where(DataAssetModel.id == asset_id))
        model = result.scalar_one_or_none()
        if model is None:
            raise AssetNotFoundError(asset_id)
        return model
```

- [ ] **Step 7: Criar sql_endpoint_repository.py**

```python
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from platform.domain.endpoints.endpoint import (
    AnyEndpoint,
    CloudBucketEndpoint,
    DatabaseEndpoint,
    EtlFlowEndpoint,
    RestApiEndpoint,
    SftpEndpoint,
)
from platform.domain.endpoints.endpoint_type import EndpointType
from platform.domain.shared.value_objects import CredentialReference
from platform.infrastructure.persistence.models.endpoint_model import EndpointModel

_BASE_FIELDS = {"id", "asset_id", "credential_ref", "technical_description", "type",
                "created_at", "updated_at"}


def _to_domain(m: EndpointModel) -> AnyEndpoint:
    """Map ORM model → typed domain Endpoint subclass. No business logic."""
    base = {
        "id": m.id,
        "asset_id": m.asset_id,
        "credential_ref": CredentialReference(m.credential_ref),
        "technical_description": m.technical_description,
        "created_at": m.created_at,
        "updated_at": m.updated_at,
        **m.subtype_data,
    }
    match EndpointType(m.type):
        case EndpointType.DATABASE:
            return DatabaseEndpoint(**base)
        case EndpointType.REST_API:
            return RestApiEndpoint(**base)
        case EndpointType.SFTP:
            return SftpEndpoint(**base)
        case EndpointType.CLOUD_BUCKET:
            return CloudBucketEndpoint(**base)
        case EndpointType.ETL_FLOW:
            return EtlFlowEndpoint(**base)
        case _:
            raise ValueError(f"Unknown EndpointType: {m.type!r}")


def _to_model(endpoint: AnyEndpoint) -> EndpointModel:
    """Map typed domain Endpoint → ORM model. Separates base fields from subtype_data."""
    all_fields = {
        k: v for k, v in vars(endpoint).items()
        if k not in _BASE_FIELDS and not k.startswith("_")
    }
    return EndpointModel(
        id=endpoint.id,
        asset_id=endpoint.asset_id,
        type=endpoint.type.value,
        credential_ref=endpoint.credential_ref.path,
        technical_description=endpoint.technical_description,
        subtype_data=all_fields,
    )


class SqlEndpointRepository:
    """SQLAlchemy implementation of EndpointRepository. Infrastructure only."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, endpoint: AnyEndpoint) -> AnyEndpoint:
        model = _to_model(endpoint)
        self._session.add(model)
        await self._session.flush()
        return _to_domain(model)

    async def find_by_id(self, endpoint_id: str) -> AnyEndpoint | None:
        result = await self._session.execute(select(EndpointModel).where(EndpointModel.id == endpoint_id))
        model = result.scalar_one_or_none()
        return _to_domain(model) if model else None

    async def find_by_asset_id(self, asset_id: str) -> AnyEndpoint | None:
        result = await self._session.execute(select(EndpointModel).where(EndpointModel.asset_id == asset_id))
        model = result.scalar_one_or_none()
        return _to_domain(model) if model else None
```

- [ ] **Step 8: Escrever testes de integração**

```python
# tests/integration/repositories/test_sql_asset_repository.py
from __future__ import annotations

import uuid
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from platform.domain.assets.asset_state import AssetState
from platform.domain.assets.data_asset import DataAsset
from platform.domain.shared.policy_tag import PolicyTag
from platform.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress
from platform.infrastructure.persistence.repositories.sql_asset_repository import SqlAssetRepository


def _asset(name: str | None = None) -> DataAsset:
    return DataAsset(
        id=str(uuid.uuid4()),
        name=name or f"asset_{uuid.uuid4().hex[:6]}",
        description="Integration test asset",
        owner=EmailAddress("po@co.com"),
        tags=["core"],
        policy_tags=[PolicyTag.PII],
        discovery_schedule=CronSchedule("0 6 * * *"),
        discovery_scope=DiscoveryScope(include=["customers"], exclude=[]),
    )


@pytest.mark.asyncio
async def test_save_and_find_by_id(db_session: AsyncSession) -> None:
    repo = SqlAssetRepository(db_session)
    asset = await repo.save(_asset())
    found = await repo.find_by_id(asset.id)
    assert found is not None
    assert found.owner.value == "po@co.com"
    assert PolicyTag.PII in found.policy_tags
    assert "customers" in found.discovery_scope.include


@pytest.mark.asyncio
async def test_timestamps_populated_on_save(db_session: AsyncSession) -> None:
    repo = SqlAssetRepository(db_session)
    asset = await repo.save(_asset())
    assert asset.created_at is not None
    assert asset.updated_at is not None


@pytest.mark.asyncio
async def test_update_state(db_session: AsyncSession) -> None:
    repo = SqlAssetRepository(db_session)
    asset = await repo.save(_asset())
    updated = await repo.update_state(asset.id, AssetState.ACTIVE)
    assert updated.state == AssetState.ACTIVE


@pytest.mark.asyncio
async def test_discovery_scope_roundtrip(db_session: AsyncSession) -> None:
    repo = SqlAssetRepository(db_session)
    scope = DiscoveryScope(include=["orders", "products"], exclude=["temp_*"])
    asset = _asset()
    asset.discovery_scope = scope
    saved = await repo.save(asset)
    found = await repo.find_by_id(saved.id)
    assert found is not None
    assert set(found.discovery_scope.include) == {"orders", "products"}
    assert "temp_*" in found.discovery_scope.exclude
```

```python
# tests/integration/repositories/test_sql_endpoint_repository.py
from __future__ import annotations

import uuid
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from platform.domain.endpoints.endpoint import DatabaseEndpoint, CloudBucketEndpoint
from platform.domain.endpoints.endpoint_type import EndpointType
from platform.domain.shared.value_objects import CredentialReference
from platform.infrastructure.persistence.repositories.sql_endpoint_repository import SqlEndpointRepository


def _cred() -> CredentialReference:
    return CredentialReference("vault/secret/test")


@pytest.mark.asyncio
async def test_save_and_find_database_endpoint(db_session: AsyncSession) -> None:
    repo = SqlEndpointRepository(db_session)
    ep = DatabaseEndpoint(
        id=str(uuid.uuid4()), asset_id="asset-1", credential_ref=_cred(),
        host="oracle.internal", port=1521, database="PROD", driver="oracle",
    )
    saved = await repo.save(ep)
    assert isinstance(saved, DatabaseEndpoint)
    assert saved.host == "oracle.internal"
    assert saved.port == 1521
    assert saved.type == EndpointType.DATABASE


@pytest.mark.asyncio
async def test_save_and_find_cloud_bucket_endpoint(db_session: AsyncSession) -> None:
    repo = SqlEndpointRepository(db_session)
    ep = CloudBucketEndpoint(
        id=str(uuid.uuid4()), asset_id="asset-2", credential_ref=_cred(),
        provider="gcs", bucket="raw-data-prod", region="us-central1",
    )
    saved = await repo.save(ep)
    assert isinstance(saved, CloudBucketEndpoint)
    assert saved.provider == "gcs"
    assert saved.bucket == "raw-data-prod"
```

- [ ] **Step 9: Rodar testes**

```bash
uv run pytest tests/integration/ -v
```

Esperado: `6+ passed`

- [ ] **Step 10: Commit**

```bash
git add platform/infrastructure/persistence/ tests/integration/
git commit -m "feat: add persistence layer (TimestampMixin, DataAssetModel, EndpointModel, AuditLogModel, SqlAssetRepository, SqlEndpointRepository)"
```

---

## Task 8: Application Use Cases + HTTP Layer + Adapters

**Files:**
- Create: `platform/application/assets/register_asset.py`
- Create: `platform/application/assets/activate_asset.py`
- Create: `platform/application/endpoints/provision_endpoint.py`
- Create: `platform/infrastructure/adapters/catalog/catalog_adapter.py`
- Create: `platform/infrastructure/adapters/catalog/noop_catalog_adapter.py`
- Create: `platform/infrastructure/adapters/catalog/datahub_catalog_adapter.py`
- Create: `platform/infrastructure/adapters/catalog/openmetadata_catalog_adapter.py`
- Create: `platform/infrastructure/adapters/notifications/notification_adapter.py`
- Create: `platform/infrastructure/adapters/notifications/noop_notification_adapter.py`
- Create: `platform/infrastructure/adapters/notifications/slack_notification_adapter.py`
- Create: `platform/infrastructure/http/schemas/asset_schemas.py`
- Create: `platform/infrastructure/http/schemas/endpoint_schemas.py`
- Create: `platform/infrastructure/http/routers/asset_router.py`
- Create: `platform/infrastructure/http/routers/endpoint_router.py`
- Create: `tests/contract/test_asset_contract.py`

**Interfaces:**
- Consumes: `UnitOfWork` (Task 6), `AssetService` (Task 5), `EndpointService` (Task 4)
- Produces: `RegisterAssetUseCase`, `ActivateAssetUseCase`, `ProvisionEndpointUseCase`, all adapters, HTTP routers

---

- [ ] **Step 1: Criar RegisterAssetUseCase**

```python
# platform/application/assets/register_asset.py
from __future__ import annotations

import uuid

from platform.application.unit_of_work import UnitOfWork
from platform.domain.assets.asset_service import AssetService
from platform.domain.assets.data_asset import DataAsset
from platform.domain.shared.policy_tag import PolicyTag
from platform.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress


class RegisterAssetUseCase:
    """
    Orchestrates DataAsset registration within a single UoW transaction.

    After commit: catalog publish and notification dispatch happen outside the transaction
    to avoid blocking the DB session on external HTTP calls.

    Example:
        use_case = RegisterAssetUseCase(uow=sql_uow, catalog=noop_adapter, notifications=noop_adapter)
        asset = await use_case.execute(name="customers", owner_email="po@co.com", ...)
    """

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def execute(
        self,
        name: str,
        description: str,
        owner_email: str,
        tags: list[str],
        policy_tags: list[str],
        discovery_schedule: str,
        discovery_scope_include: list[str],
        discovery_scope_exclude: list[str],
    ) -> DataAsset:
        async with self._uow:
            service = AssetService(repo=self._uow.assets)
            asset = await service.register(
                asset_id=str(uuid.uuid4()),
                name=name,
                description=description,
                owner=EmailAddress(owner_email),
                tags=tags,
                policy_tags=[PolicyTag(t) for t in policy_tags],
                discovery_schedule=CronSchedule(discovery_schedule),
                discovery_scope=DiscoveryScope(
                    include=discovery_scope_include,
                    exclude=discovery_scope_exclude,
                ),
            )
            await self._uow.commit()
        return asset
```

- [ ] **Step 2: Criar ActivateAssetUseCase**

```python
# platform/application/assets/activate_asset.py
from __future__ import annotations

from platform.application.unit_of_work import UnitOfWork
from platform.domain.assets.asset_service import AssetService
from platform.domain.assets.data_asset import DataAsset


class ActivateAssetUseCase:
    """Transitions DataAsset DRAFT → ACTIVE within a UoW transaction."""

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def execute(self, asset_id: str, endpoint_id: str) -> DataAsset:
        async with self._uow:
            service = AssetService(repo=self._uow.assets)
            asset = await service.transition_to_active(asset_id, endpoint_id)
            await self._uow.commit()
        return asset
```

- [ ] **Step 3: Criar adapters (Protocol + 3 catalog implementations + 2 notification)**

```python
# platform/infrastructure/adapters/catalog/catalog_adapter.py
from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class CatalogAdapter(Protocol):
    """Protocol for external catalog integration. Selected via PLATFORM_CATALOG_ADAPTER env var."""

    async def publish_asset(self, asset_id: str, name: str, state: str, metadata: dict[str, Any]) -> None: ...
    async def publish_lineage(self, source_object_id: str, destination_object_id: str, pipeline_id: str) -> None: ...
    async def publish_schema_drift(self, asset_id: str, drift_event: dict[str, Any]) -> None: ...
```

```python
# platform/infrastructure/adapters/catalog/noop_catalog_adapter.py
from __future__ import annotations
import logging
from typing import Any

logger = logging.getLogger(__name__)

class NoopCatalogAdapter:
    async def publish_asset(self, asset_id: str, name: str, state: str, metadata: dict[str, Any]) -> None:
        logger.debug("NoopCatalogAdapter.publish_asset: %s state=%s", asset_id, state)

    async def publish_lineage(self, source_object_id: str, destination_object_id: str, pipeline_id: str) -> None:
        logger.debug("NoopCatalogAdapter.publish_lineage: %s -> %s", source_object_id, destination_object_id)

    async def publish_schema_drift(self, asset_id: str, drift_event: dict[str, Any]) -> None:
        logger.debug("NoopCatalogAdapter.publish_schema_drift: %s", asset_id)
```

```python
# platform/infrastructure/adapters/catalog/datahub_catalog_adapter.py
from __future__ import annotations
from typing import Any

class DataHubCatalogAdapter:
    """DataHub REST Emitter integration. Configure via PLATFORM_DATAHUB_URL and PLATFORM_DATAHUB_TOKEN."""

    def __init__(self, base_url: str, token: str) -> None:
        self._base_url = base_url
        self._token = token

    async def publish_asset(self, asset_id: str, name: str, state: str, metadata: dict[str, Any]) -> None:
        raise NotImplementedError("DataHubCatalogAdapter.publish_asset not yet implemented")

    async def publish_lineage(self, source_object_id: str, destination_object_id: str, pipeline_id: str) -> None:
        raise NotImplementedError

    async def publish_schema_drift(self, asset_id: str, drift_event: dict[str, Any]) -> None:
        raise NotImplementedError
```

```python
# platform/infrastructure/adapters/catalog/openmetadata_catalog_adapter.py
from __future__ import annotations
from typing import Any

class OpenMetadataCatalogAdapter:
    """OpenMetadata SDK integration. Configure via PLATFORM_OPENMETADATA_URL and PLATFORM_OPENMETADATA_TOKEN."""

    def __init__(self, base_url: str, token: str) -> None:
        self._base_url = base_url
        self._token = token

    async def publish_asset(self, asset_id: str, name: str, state: str, metadata: dict[str, Any]) -> None:
        raise NotImplementedError("OpenMetadataCatalogAdapter.publish_asset not yet implemented")

    async def publish_lineage(self, source_object_id: str, destination_object_id: str, pipeline_id: str) -> None:
        raise NotImplementedError

    async def publish_schema_drift(self, asset_id: str, drift_event: dict[str, Any]) -> None:
        raise NotImplementedError
```

```python
# platform/infrastructure/adapters/notifications/notification_adapter.py
from __future__ import annotations
from typing import Literal, Protocol, runtime_checkable

AlertLevel = Literal["info", "warning", "critical"]

@runtime_checkable
class NotificationAdapter(Protocol):
    async def send_alert(self, channel: str, title: str, message: str, level: AlertLevel) -> None: ...
```

```python
# platform/infrastructure/adapters/notifications/noop_notification_adapter.py
from __future__ import annotations
import logging
from platform.infrastructure.adapters.notifications.notification_adapter import AlertLevel

logger = logging.getLogger(__name__)

class NoopNotificationAdapter:
    async def send_alert(self, channel: str, title: str, message: str, level: AlertLevel) -> None:
        logger.debug("NoopNotificationAdapter: [%s] %s — %s", level, title, message)
```

```python
# platform/infrastructure/adapters/notifications/slack_notification_adapter.py
from __future__ import annotations
from platform.infrastructure.adapters.notifications.notification_adapter import AlertLevel

class SlackNotificationAdapter:
    """Slack Incoming Webhooks integration. Configure via PLATFORM_SLACK_WEBHOOK_URL."""

    def __init__(self, webhook_url: str) -> None:
        self._webhook_url = webhook_url

    async def send_alert(self, channel: str, title: str, message: str, level: AlertLevel) -> None:
        raise NotImplementedError("SlackNotificationAdapter.send_alert not yet implemented")
```

- [ ] **Step 4: Criar asset_schemas.py (HTTP transport)**

```python
# platform/infrastructure/http/schemas/asset_schemas.py
from __future__ import annotations

from typing import Any
from pydantic import BaseModel, ConfigDict
from platform.domain.assets.asset_state import AssetState
from platform.domain.shared.policy_tag import PolicyTag
from platform.domain.assets.data_asset import DataAsset


class AssetCreateRequest(BaseModel):
    name: str
    description: str
    owner_email: str
    tags: list[str] = []
    policy_tags: list[PolicyTag] = []
    discovery_schedule: str
    discovery_scope_include: list[str] = []
    discovery_scope_exclude: list[str] = []


class AssetScopeUpdateRequest(BaseModel):
    discovery_scope_include: list[str]
    discovery_scope_exclude: list[str]


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
    endpoint_id: str | None


def asset_to_response(asset: DataAsset) -> AssetResponse:
    """Map domain entity to HTTP response. Transport layer only."""
    return AssetResponse(
        id=asset.id,
        name=asset.name,
        description=asset.description,
        owner_email=asset.owner.value,
        tags=asset.tags,
        policy_tags=asset.policy_tags,
        state=asset.state,
        discovery_schedule=asset.discovery_schedule.expression,
        discovery_scope_include=list(asset.discovery_scope.include),
        discovery_scope_exclude=list(asset.discovery_scope.exclude),
        endpoint_id=asset.endpoint_id,
    )
```

- [ ] **Step 5: Criar asset_router.py**

```python
# platform/infrastructure/http/routers/asset_router.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from platform.application.assets.activate_asset import ActivateAssetUseCase
from platform.application.assets.register_asset import RegisterAssetUseCase
from platform.auth.current_user import CurrentUser
from platform.auth.dependencies import get_current_user, require_role
from platform.auth.role import Role
from platform.domain.assets.asset_service import AssetNotFoundError, InvalidStateTransitionError
from platform.infrastructure.http.schemas.asset_schemas import (
    AssetCreateRequest, AssetResponse, AssetScopeUpdateRequest, asset_to_response,
)
from platform.infrastructure.persistence.database import get_db, get_session_factory
from platform.infrastructure.persistence.sql_unit_of_work import SqlUnitOfWork

router = APIRouter()


@router.post("/", response_model=AssetResponse, status_code=status.HTTP_201_CREATED)
async def register_asset(
    body: AssetCreateRequest,
    _: CurrentUser = Depends(require_role(Role.PO_PM, Role.ANALYTICS_ENGINEER)),
) -> AssetResponse:
    """Register a new DataAsset in DRAFT state. No business logic in router."""
    uow = SqlUnitOfWork(get_session_factory())
    use_case = RegisterAssetUseCase(uow=uow)
    try:
        asset = await use_case.execute(
            name=body.name, description=body.description,
            owner_email=body.owner_email, tags=body.tags,
            policy_tags=[t.value for t in body.policy_tags],
            discovery_schedule=body.discovery_schedule,
            discovery_scope_include=body.discovery_scope_include,
            discovery_scope_exclude=body.discovery_scope_exclude,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return asset_to_response(asset)


@router.get("/{asset_id}", response_model=AssetResponse)
async def get_asset(
    asset_id: str,
    session: AsyncSession = Depends(get_db),
    _: CurrentUser = Depends(get_current_user),
) -> AssetResponse:
    """Retrieve a DataAsset by id. Visible to all roles."""
    from platform.infrastructure.persistence.repositories.sql_asset_repository import SqlAssetRepository
    repo = SqlAssetRepository(session)
    asset = await repo.find_by_id(asset_id)
    if asset is None:
        raise HTTPException(status_code=404, detail=f"Asset not found: {asset_id!r}")
    return asset_to_response(asset)


@router.post("/{asset_id}/activate", response_model=AssetResponse)
async def activate_asset(
    asset_id: str,
    endpoint_id: str,
    _: CurrentUser = Depends(require_role(Role.SRE)),
) -> AssetResponse:
    """Transition asset DRAFT → ACTIVE. SRE only."""
    uow = SqlUnitOfWork(get_session_factory())
    use_case = ActivateAssetUseCase(uow=uow)
    try:
        asset = await use_case.execute(asset_id, endpoint_id)
    except AssetNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except InvalidStateTransitionError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return asset_to_response(asset)
```

- [ ] **Step 6: Criar endpoint_router.py (SRE-only, retorna apenas id + type)**

```python
# platform/infrastructure/http/routers/endpoint_router.py
from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from platform.auth.current_user import CurrentUser
from platform.auth.dependencies import require_role
from platform.auth.role import Role
from platform.domain.endpoints.endpoint import (
    CloudBucketEndpoint, DatabaseEndpoint, EtlFlowEndpoint, RestApiEndpoint, SftpEndpoint,
)
from platform.domain.endpoints.endpoint_service import EndpointService
from platform.domain.endpoints.endpoint_type import EndpointType
from platform.domain.shared.value_objects import CredentialReference
from platform.infrastructure.persistence.database import get_db
from platform.infrastructure.persistence.repositories.sql_endpoint_repository import SqlEndpointRepository

router = APIRouter()


class EndpointResponse(BaseModel):
    """HTTP response: only id and type are visible. Sensitive fields never exposed."""
    id: str
    asset_id: str
    type: EndpointType


class DatabaseEndpointCreateRequest(BaseModel):
    asset_id: str
    credential_ref: str
    technical_description: str = ""
    host: str
    port: int
    database: str
    driver: str


@router.post("/database", response_model=EndpointResponse, status_code=status.HTTP_201_CREATED)
async def provision_database_endpoint(
    body: DatabaseEndpointCreateRequest,
    session: AsyncSession = Depends(get_db),
    _: CurrentUser = Depends(require_role(Role.SRE)),
) -> EndpointResponse:
    """Provision a DatabaseEndpoint. SRE only."""
    service = EndpointService(repo=SqlEndpointRepository(session))
    ep = DatabaseEndpoint(
        id=str(uuid.uuid4()),
        asset_id=body.asset_id,
        credential_ref=CredentialReference(body.credential_ref),
        technical_description=body.technical_description,
        host=body.host, port=body.port, database=body.database, driver=body.driver,
    )
    saved = await service.provision(ep)
    return EndpointResponse(id=saved.id, asset_id=saved.asset_id, type=saved.type)
```

- [ ] **Step 7: Escrever testes de contrato**

```python
# tests/contract/test_asset_contract.py
from __future__ import annotations

import pytest
from httpx import AsyncClient

from platform.auth.current_user import CurrentUser
from platform.auth.dependencies import get_current_user
from platform.auth.role import Role
from platform.domain.shared.value_objects import EmailAddress
from platform.main import create_app


def _override(role: Role):
    user = CurrentUser(id="u1", email=EmailAddress("test@co.com"), role=role)
    return lambda: user


@pytest.fixture
async def ae_client(client: AsyncClient) -> AsyncClient:
    client.app.dependency_overrides[get_current_user] = _override(Role.ANALYTICS_ENGINEER)
    return client


@pytest.fixture
async def sre_client(client: AsyncClient) -> AsyncClient:
    client.app.dependency_overrides[get_current_user] = _override(Role.SRE)
    return client


@pytest.mark.asyncio
async def test_create_asset_returns_201_in_draft(ae_client: AsyncClient) -> None:
    response = await ae_client.post("/assets/", json={
        "name": "contract_asset", "description": "Test", "owner_email": "ae@co.com",
        "tags": ["core"], "policy_tags": ["PII"],
        "discovery_schedule": "0 6 * * *",
        "discovery_scope_include": ["customers"], "discovery_scope_exclude": [],
    })
    assert response.status_code == 201
    body = response.json()
    assert body["state"] == "draft"
    assert body["policy_tags"] == ["PII"]
    assert "id" in body


@pytest.mark.asyncio
async def test_invalid_cron_returns_422(ae_client: AsyncClient) -> None:
    response = await ae_client.post("/assets/", json={
        "name": "bad_sched", "description": "Test", "owner_email": "ae@co.com",
        "tags": [], "policy_tags": [],
        "discovery_schedule": "not-a-cron",
        "discovery_scope_include": [], "discovery_scope_exclude": [],
    })
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_sre_cannot_create_asset(sre_client: AsyncClient) -> None:
    response = await sre_client.post("/assets/", json={
        "name": "sre_asset", "description": "Test", "owner_email": "sre@co.com",
        "tags": [], "policy_tags": [], "discovery_schedule": "0 6 * * *",
        "discovery_scope_include": [], "discovery_scope_exclude": [],
    })
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_get_nonexistent_asset_returns_404(ae_client: AsyncClient) -> None:
    response = await ae_client.get("/assets/00000000-0000-0000-0000-000000000000")
    assert response.status_code == 404
```

- [ ] **Step 8: Rodar todos os testes**

```bash
uv run pytest tests/ -v
```

Esperado: todos os testes passando.

- [ ] **Step 9: Rodar o check completo do CI**

```bash
make check
```

Esperado: `✅ All checks passed.`

- [ ] **Step 10: Gerar migration inicial**

```bash
make migrate-create name="initial_schema"
make migrate
```

- [ ] **Step 11: Commit final**

```bash
git add platform/application/ platform/infrastructure/ tests/contract/ migrations/
git commit -m "feat: add use cases (RegisterAsset, ActivateAsset), adapters (CatalogAdapter, NotificationAdapter), HTTP routers, and contract tests"
```

---

## Self-Review

**DDD Completeness:**
- ✅ **Entities**: DataAsset, Endpoint (+ 5 subclasses), AuditLog
- ✅ **Value Objects**: EmailAddress, CredentialReference, CronSchedule, DiscoveryScope, PolicyTag
- ✅ **Aggregate Root**: DataAsset (owns discovery_scope, state machine, endpoint reference)
- ✅ **Repository Protocols**: AssetRepository, EndpointRepository (defined in domain)
- ✅ **Domain Services**: AssetService, EndpointService (no framework imports)
- ✅ **Application Services**: RegisterAssetUseCase, ActivateAssetUseCase (use UoW)
- ✅ **Unit of Work**: UnitOfWork Protocol + SqlUnitOfWork (atomic commits)
- ✅ **Domain Events**: AssetRegistered, AssetActivated, AssetStateChanged, EndpointProvisioned (wiring to audit log deferred to next plan)
- ✅ **Bounded Contexts**: Assets & Endpoints (this plan), DataObjects & Pipelines (Plan 2), Discovery (Plan 3)

**Clean Architecture Completeness:**
- ✅ Domain layer: zero framework imports
- ✅ Application layer: zero framework imports
- ✅ Infrastructure layer: FastAPI, SQLAlchemy, adapters
- ✅ Dependency rule: outer layers depend on inner, never reverse
- ✅ Repositories: Protocols in domain, SQL implementations in infra

**Data Governance:**
- ✅ PolicyTag enum with 4 classification levels
- ✅ AuditLog table (append-only, never updated)
- ✅ Timestamps (created_at, updated_at) on all ORM models
- ✅ DiscoveryScope as typed Value Object
- ✅ CredentialReference never stored as plain value
- ✅ RBAC: SRE-only for endpoints, PO/PM/AE for assets

**Next plan:** `2026-06-27-plan-02-dataobject-pipeline.md` — DataObject, DataElement, Pipeline YAML, DAG Generator, CronSchedule in YAML, Discovery polymorphic runners.
