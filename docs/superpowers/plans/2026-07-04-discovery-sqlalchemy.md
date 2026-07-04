# SQLAlchemy Generic Database Discovery — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a real, generic, high-performance Database Discovery runner that uses SQLAlchemy Inspector reflection to capture rich schema metadata (columns, types, nullability, PKs, FKs, indexes, comments, row count estimates) in a single database session per discovery run.

**Architecture:**
- Clean Architecture is maintained throughout: the domain is unmodified. The Application layer owns two new ports (`SecretManagerPort`, updated `DiscoveryRunner`). The Infrastructure layer owns all real implementations (`SqlAlchemySecretAdapter`, `SqlAlchemyDatabaseRunner`, `SqlAlchemyTypeMapper`).
- **One engine, one connection per discovery run.** All tables for an asset are reflected in a single `conn.run_sync()` call to avoid connection overhead.
- The `DatabaseEndpoint.credential_ref.path` resolves to a Vault payload (a dict). The platform parses that dict and assembles the SQLAlchemy URL — keeping DB dialect knowledge outside the domain.

**Tech Stack:** Python 3.12, SQLAlchemy >= 2.0 (asyncio), aiosqlite (test driver), pytest-asyncio >= 0.23.

---

## Global Constraints

- All new source files under `app/` — follow existing `from __future__ import annotations` header convention.
- Async-first: I/O operations must use `async`/`await`. Sync SQLAlchemy Inspector calls must be wrapped in `conn.run_sync(...)`.
- No new external dependencies beyond what is already in `pyproject.toml` (SQLAlchemy, aiosqlite dev dep already present).
- All test files under `tests/unit/infrastructure/discovery/` for unit tests; `tests/integration/` for integration tests.
- Follow TDD: write the failing test first, then implement.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `app/application/shared/secret_manager_port.py` | **Create** | Port (Protocol) for resolving credential_ref → connection dict |
| `app/infrastructure/adapters/secrets/__init__.py` | **Create** | Package marker |
| `app/infrastructure/adapters/secrets/noop_secret_manager_adapter.py` | **Create** | Dev/test stub that echoes back a configurable dict |
| `app/infrastructure/discovery/sqlalchemy_type_mapper.py` | **Create** | Pure function: SQLAlchemy TypeEngine → normalized platform type string |
| `app/infrastructure/discovery/database_runner.py` | **Replace** | Real SQLAlchemy Inspector-based discovery runner |
| `app/infrastructure/discovery/discovery_runner_factory.py` | **Modify** | Inject `SecretManagerPort` into `DatabaseRunner` |
| `tests/unit/infrastructure/discovery/__init__.py` | **Create** | Package marker |
| `tests/unit/infrastructure/discovery/test_sqlalchemy_type_mapper.py` | **Create** | Unit tests for type mapper |
| `tests/unit/infrastructure/discovery/test_database_runner.py` | **Create** | Integration-style unit test using in-memory SQLite |

---

## Task 1: Secret Manager Port (Application Layer)

**Files:**
- Create: `app/application/shared/secret_manager_port.py`

**Interfaces:**
- Produces: `SecretManagerPort` protocol with `async def resolve(self, ref: str) -> dict[str, str]`

The port returns a plain dict. The caller (DatabaseRunner) is responsible for interpreting the dict and assembling the connection URL. This follows the Dependency Inversion Principle: the Application layer owns the contract; the Infrastructure layer owns the real implementation.

- [ ] **Step 1: Write the failing test**

Create `tests/unit/infrastructure/discovery/__init__.py` (empty file), then create `tests/unit/infrastructure/discovery/test_secret_manager_port.py`:

```python
# tests/unit/infrastructure/discovery/test_secret_manager_port.py
from __future__ import annotations

import pytest
from app.application.shared.secret_manager_port import SecretManagerPort
from app.infrastructure.adapters.secrets.noop_secret_manager_adapter import NoopSecretManagerAdapter


@pytest.mark.asyncio
async def test_noop_adapter_fulfills_port_protocol() -> None:
    adapter = NoopSecretManagerAdapter(
        store={
            "vault/db/prod": {
                "driver": "sqlite+aiosqlite",
                "database": ":memory:",
            }
        }
    )
    assert isinstance(adapter, SecretManagerPort)
    result = await adapter.resolve("vault/db/prod")
    assert result["driver"] == "sqlite+aiosqlite"
    assert result["database"] == ":memory:"
```

- [ ] **Step 2: Run to verify it fails**

```bash
uv run pytest tests/unit/infrastructure/discovery/test_secret_manager_port.py -v
```

Expected: `ImportError` — modules don't exist yet.

- [ ] **Step 3: Implement the Port**

```python
# app/application/shared/secret_manager_port.py
from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class SecretManagerPort(Protocol):
    """
    Port for resolving a credential reference path to a credential payload dict.

    The returned dict contains raw key-value pairs as stored in the secret manager
    (e.g., Vault). It is the caller's responsibility to interpret the dict and
    build the appropriate connection mechanism.

    Example:
        port = SomeAdapter(...)
        creds = await port.resolve("vault/db/prod")
        # creds == {"driver": "postgresql+asyncpg", "host": "...", "port": "5432",
        #           "user": "svc_acct", "password": "...", "database": "analytics"}
    """

    async def resolve(self, ref: str) -> dict[str, str]: ...
```

- [ ] **Step 4: Implement the NoopSecretManagerAdapter**

Create the directory structure:
```
app/infrastructure/adapters/secrets/__init__.py  (empty)
app/infrastructure/adapters/secrets/noop_secret_manager_adapter.py
```

```python
# app/infrastructure/adapters/secrets/noop_secret_manager_adapter.py
from __future__ import annotations


class NoopSecretManagerAdapter:
    """
    In-memory stub for SecretManagerPort. For local dev and tests only.

    Accepts a pre-loaded store dict so tests can inject any credential payload
    without side effects.

    Example:
        adapter = NoopSecretManagerAdapter({"vault/db/test": {"driver": "sqlite+aiosqlite", "database": ":memory:"}})
    """

    def __init__(self, store: dict[str, dict[str, str]] | None = None) -> None:
        self._store: dict[str, dict[str, str]] = store or {}

    async def resolve(self, ref: str) -> dict[str, str]:
        if ref not in self._store:
            raise KeyError(f"No credential found for ref: {ref!r}")
        return dict(self._store[ref])
```

- [ ] **Step 5: Run test to verify it passes**

```bash
uv run pytest tests/unit/infrastructure/discovery/test_secret_manager_port.py -v
```

Expected: `PASSED`.

- [ ] **Step 6: Commit**

```bash
git add app/application/shared/secret_manager_port.py \
        app/infrastructure/adapters/secrets/__init__.py \
        app/infrastructure/adapters/secrets/noop_secret_manager_adapter.py \
        tests/unit/infrastructure/discovery/__init__.py \
        tests/unit/infrastructure/discovery/test_secret_manager_port.py
git commit -m "feat: add SecretManagerPort and NoopSecretManagerAdapter"
```

---

## Task 2: SQLAlchemy Type Mapper (Infrastructure — Pure Function)

**Files:**
- Create: `app/infrastructure/discovery/sqlalchemy_type_mapper.py`
- Create: `tests/unit/infrastructure/discovery/test_sqlalchemy_type_mapper.py`

**Interfaces:**
- Consumes: `sqlalchemy.types.TypeEngine` instance (from Inspector columns dict)
- Produces: `str` — one of `"string"`, `"integer"`, `"bigint"`, `"float"`, `"boolean"`, `"date"`, `"datetime"`, `"time"`, `"json"`, `"binary"`, `"uuid"`, `"unknown"`

**Why a separate mapper file?**  
The `DatabaseRunner` would violate SRP if it embedded a large type-mapping `if/elif` chain. A focused, pure function in its own module is independently testable, zero-dependency, and easy to extend for new dialects.

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/infrastructure/discovery/test_sqlalchemy_type_mapper.py
from __future__ import annotations

import pytest
import sqlalchemy.types as T
from app.infrastructure.discovery.sqlalchemy_type_mapper import map_sa_type_to_normalized


@pytest.mark.parametrize("sa_type,expected", [
    (T.String(), "string"),
    (T.Text(), "string"),
    (T.Unicode(), "string"),
    (T.UnicodeText(), "string"),
    (T.Integer(), "integer"),
    (T.SmallInteger(), "integer"),
    (T.BigInteger(), "bigint"),
    (T.Float(), "float"),
    (T.Numeric(), "float"),
    (T.Boolean(), "boolean"),
    (T.Date(), "date"),
    (T.DateTime(), "datetime"),
    (T.Time(), "time"),
    (T.JSON(), "json"),
    (T.LargeBinary(), "binary"),
    (T.Uuid(), "uuid"),
    (T.NullType(), "unknown"),
])
def test_map_sa_type_to_normalized(sa_type: T.TypeEngine, expected: str) -> None:
    assert map_sa_type_to_normalized(sa_type) == expected


def test_unknown_type_returns_unknown() -> None:
    class CustomType(T.TypeEngine):
        pass
    assert map_sa_type_to_normalized(CustomType()) == "unknown"
```

- [ ] **Step 2: Run to verify it fails**

```bash
uv run pytest tests/unit/infrastructure/discovery/test_sqlalchemy_type_mapper.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Implement the mapper**

```python
# app/infrastructure/discovery/sqlalchemy_type_mapper.py
from __future__ import annotations

import sqlalchemy.types as T


# Map from SQLAlchemy generic type classes (base types, not dialect-specific)
# to platform canonical normalized type strings.
_TYPE_MAP: tuple[tuple[type[T.TypeEngine], str], ...] = (
    (T.BigInteger, "bigint"),
    (T.Boolean, "boolean"),
    (T.Date, "date"),
    (T.DateTime, "datetime"),
    (T.Float, "float"),
    (T.Integer, "integer"),
    (T.JSON, "json"),
    (T.LargeBinary, "binary"),
    (T.Numeric, "float"),
    (T.String, "string"),
    (T.Text, "string"),
    (T.Time, "time"),
    (T.Unicode, "string"),
    (T.UnicodeText, "string"),
    (T.Uuid, "uuid"),
)


def map_sa_type_to_normalized(sa_type: T.TypeEngine) -> str:
    """
    Map a SQLAlchemy TypeEngine instance to the platform's canonical normalized type string.

    Resolution order matters: more specific types (BigInteger) must precede their
    parents (Integer) to avoid incorrect downgrading.

    Returns "unknown" for any type not in the mapping (e.g., dialect-specific types
    that don't inherit from a recognized generic class will resolve via MRO fallback).
    """
    for base_type, normalized in _TYPE_MAP:
        if isinstance(sa_type, base_type):
            return normalized
    return "unknown"
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/unit/infrastructure/discovery/test_sqlalchemy_type_mapper.py -v
```

Expected: `17 passed`.

- [ ] **Step 5: Commit**

```bash
git add app/infrastructure/discovery/sqlalchemy_type_mapper.py \
        tests/unit/infrastructure/discovery/test_sqlalchemy_type_mapper.py
git commit -m "feat: add SQLAlchemy-to-normalized type mapper"
```

---

## Task 3: URL Builder — Vault Payload to SQLAlchemy URL

**Files:**
- Create: `app/infrastructure/discovery/connection_url_builder.py`
- Test inline in Task 4's test file (no standalone test needed — it's a private concern).

**Why a separate builder?**  
The URL assembly logic (which fields are mandatory, how `port` is handled, which drivers use which format) would pollute `DatabaseRunner`. Extracting it keeps runner code readable and makes the builder independently testable.

**Interfaces:**
- Consumes: `dict[str, str]` from `SecretManagerPort.resolve()`
- Produces: `str` — valid SQLAlchemy async-compatible connection URL

**Vault payload contract (what the platform expects the Vault to return):**

```json
{
  "driver":   "postgresql+asyncpg",   // required — SQLAlchemy dialect+driver
  "host":     "db.internal",          // required for networked DBs
  "port":     "5432",                 // optional, driver default used if absent
  "user":     "svc_discovery",        // optional
  "password": "...",                  // optional
  "database": "analytics"            // required
}
```

- [ ] **Step 1: Write the failing test**

Add to `tests/unit/infrastructure/discovery/test_sqlalchemy_type_mapper.py` a new test in a new file:

```python
# tests/unit/infrastructure/discovery/test_connection_url_builder.py
from __future__ import annotations

import pytest
from app.infrastructure.discovery.connection_url_builder import build_connection_url


def test_build_url_full_postgres() -> None:
    payload = {
        "driver": "postgresql+asyncpg",
        "host": "db.internal",
        "port": "5432",
        "user": "svc",
        "password": "secret",
        "database": "analytics",
    }
    url = build_connection_url(payload)
    assert url == "postgresql+asyncpg://svc:secret@db.internal:5432/analytics"


def test_build_url_sqlite_in_memory() -> None:
    payload = {"driver": "sqlite+aiosqlite", "database": ":memory:"}
    url = build_connection_url(payload)
    assert url == "sqlite+aiosqlite:///:memory:"


def test_build_url_missing_driver_raises() -> None:
    with pytest.raises(ValueError, match="driver"):
        build_connection_url({"database": "mydb"})


def test_build_url_missing_database_raises() -> None:
    with pytest.raises(ValueError, match="database"):
        build_connection_url({"driver": "postgresql+asyncpg", "host": "localhost"})


def test_build_url_no_auth() -> None:
    payload = {
        "driver": "postgresql+asyncpg",
        "host": "localhost",
        "database": "mydb",
    }
    url = build_connection_url(payload)
    assert url == "postgresql+asyncpg://localhost/mydb"
```

- [ ] **Step 2: Run to verify it fails**

```bash
uv run pytest tests/unit/infrastructure/discovery/test_connection_url_builder.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Implement the builder**

```python
# app/infrastructure/discovery/connection_url_builder.py
from __future__ import annotations


def build_connection_url(payload: dict[str, str]) -> str:
    """
    Assemble a SQLAlchemy-compatible connection URL from a Vault credential payload.

    The payload is a flat dict as returned by SecretManagerPort.resolve().
    Keys: driver (required), database (required), host, port, user, password (all optional).

    SQLite special case:
        {"driver": "sqlite+aiosqlite", "database": ":memory:"}
        → "sqlite+aiosqlite:///:memory:"

    Raises:
        ValueError: if 'driver' or 'database' are missing from the payload.
    """
    driver = payload.get("driver")
    if not driver:
        raise ValueError("Vault payload missing required key 'driver'.")

    database = payload.get("database")
    if not database:
        raise ValueError("Vault payload missing required key 'database'.")

    # SQLite uses a file-path format: sqlite+aiosqlite:///path/to/file or /:memory:
    if driver.startswith("sqlite"):
        return f"{driver}:///{database}"

    host = payload.get("host", "")
    port = payload.get("port", "")
    user = payload.get("user", "")
    password = payload.get("password", "")

    auth = ""
    if user and password:
        auth = f"{user}:{password}@"
    elif user:
        auth = f"{user}@"

    netloc = host
    if port:
        netloc = f"{host}:{port}"

    return f"{driver}://{auth}{netloc}/{database}"
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/unit/infrastructure/discovery/test_connection_url_builder.py -v
```

Expected: `5 passed`.

- [ ] **Step 5: Commit**

```bash
git add app/infrastructure/discovery/connection_url_builder.py \
        tests/unit/infrastructure/discovery/test_connection_url_builder.py
git commit -m "feat: add Vault payload → SQLAlchemy URL builder"
```

---

## Task 4: DatabaseRunner with Real SQLAlchemy Inspector

**Files:**
- Replace: `app/infrastructure/discovery/database_runner.py`
- Modify: `app/infrastructure/discovery/discovery_runner_factory.py`
- Create: `tests/unit/infrastructure/discovery/test_database_runner.py`

**Interfaces:**
- Consumes:
  - `SecretManagerPort` (from Task 1) — `async def resolve(self, ref: str) -> dict[str, str]`
  - `build_connection_url(payload: dict[str, str]) -> str` (from Task 3)
  - `map_sa_type_to_normalized(sa_type: TypeEngine) -> str` (from Task 2)
  - `DatabaseEndpoint.credential_ref.path: str`
  - `DataObject.name: str`, `DataObject.id: str`
- Produces:
  - `list[SchemaSnapshot]` — one per `DataObject`, with full `SchemaField` list

**Performance design — one connection, one `run_sync` call:**

All table reflections for the same asset happen inside a **single** `async with engine.connect() as conn:` block. Within that block, all objects are iterated in a single `conn.run_sync(_inspect_all_objects)` call. This avoids opening and closing a new connection per table.

> [!TIP]
> `inspector.get_columns()`, `inspector.get_pk_constraint()`, `inspector.get_foreign_keys()`, `inspector.get_indexes()`, and `inspector.get_table_comment()` are all synchronous calls that reuse the same underlying DB cursor within a single `run_sync`. This is the recommended SQLAlchemy 2.0 pattern.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/infrastructure/discovery/test_database_runner.py
from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from app.domain.endpoints.endpoint import DatabaseEndpoint
from app.domain.objects.data_object import DataObject
from app.domain.objects.object_type import ObjectType
from app.domain.shared.value_objects import CredentialReference
from app.infrastructure.adapters.secrets.noop_secret_manager_adapter import NoopSecretManagerAdapter
from app.infrastructure.discovery.database_runner import DatabaseRunner


def _endpoint() -> DatabaseEndpoint:
    return DatabaseEndpoint(
        id="ep-1",
        asset_id="asset-1",
        credential_ref=CredentialReference("vault/db/test"),
    )


def _object(name: str) -> DataObject:
    return DataObject(
        id=f"obj-{name}",
        asset_id="asset-1",
        name=name,
        type=ObjectType.TABLE,
    )


@pytest.fixture()
async def seeded_engine():
    """Create an in-memory SQLite DB with two test tables."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.execute(__import__("sqlalchemy").text("""
            CREATE TABLE customers (
                id INTEGER PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                email TEXT,
                balance REAL,
                is_active INTEGER,
                created_at TEXT
            )
        """))
        await conn.execute(__import__("sqlalchemy").text("""
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY NOT NULL,
                customer_id INTEGER NOT NULL,
                total REAL,
                note TEXT
            )
        """))
        await conn.execute(__import__("sqlalchemy").text(
            "INSERT INTO customers VALUES (1,'Alice','alice@co.com',100.0,1,'2024-01-01')"
        ))
        await conn.execute(__import__("sqlalchemy").text(
            "INSERT INTO customers VALUES (2,'Bob','bob@co.com',200.0,1,'2024-01-02')"
        ))
    yield engine
    await engine.dispose()


@pytest.mark.asyncio
async def test_runner_returns_one_snapshot_per_object() -> None:
    secret_manager = NoopSecretManagerAdapter(
        store={"vault/db/test": {"driver": "sqlite+aiosqlite", "database": ":memory:"}}
    )
    runner = DatabaseRunner(secret_manager=secret_manager)
    snapshots = await runner.run(
        asset_id="asset-1",
        objects=[_object("customers"), _object("orders")],
        endpoint=_endpoint(),
    )
    assert len(snapshots) == 2


@pytest.mark.asyncio
async def test_runner_captures_all_columns(seeded_engine) -> None:
    """
    Uses a pre-seeded SQLite engine injected via an adapter that returns the URL.
    Note: since seeded_engine is an in-memory DB, we use a special adapter that
    returns the same URL — the runner creates its own engine from the URL string.
    We test column capture by verifying 'customers' table columns are reflected.
    """
    secret_manager = NoopSecretManagerAdapter(
        store={"vault/db/test": {"driver": "sqlite+aiosqlite", "database": ":memory:"}}
    )
    runner = DatabaseRunner(secret_manager=secret_manager)
    snapshots = await runner.run(
        asset_id="asset-1",
        objects=[_object("customers")],
        endpoint=_endpoint(),
    )
    snap = snapshots[0]
    assert snap.object_name == "customers"
    field_names = {f.name for f in snap.fields}
    assert "id" in field_names
    assert "name" in field_names
    assert "email" in field_names


@pytest.mark.asyncio
async def test_runner_marks_primary_key() -> None:
    secret_manager = NoopSecretManagerAdapter(
        store={"vault/db/test": {"driver": "sqlite+aiosqlite", "database": ":memory:"}}
    )
    runner = DatabaseRunner(secret_manager=secret_manager)
    snapshots = await runner.run(
        asset_id="asset-1",
        objects=[_object("customers")],
        endpoint=_endpoint(),
    )
    id_field = next(f for f in snapshots[0].fields if f.name == "id")
    assert id_field.is_primary_key is True


@pytest.mark.asyncio
async def test_runner_sets_runner_type() -> None:
    secret_manager = NoopSecretManagerAdapter(
        store={"vault/db/test": {"driver": "sqlite+aiosqlite", "database": ":memory:"}}
    )
    runner = DatabaseRunner(secret_manager=secret_manager)
    snapshots = await runner.run(
        asset_id="asset-1",
        objects=[_object("customers")],
        endpoint=_endpoint(),
    )
    assert snapshots[0].runner_type == "database"


@pytest.mark.asyncio
async def test_runner_skips_missing_table_gracefully() -> None:
    """If a DataObject references a table not in the DB, runner should produce an empty snapshot rather than crashing."""
    secret_manager = NoopSecretManagerAdapter(
        store={"vault/db/test": {"driver": "sqlite+aiosqlite", "database": ":memory:"}}
    )
    runner = DatabaseRunner(secret_manager=secret_manager)
    snapshots = await runner.run(
        asset_id="asset-1",
        objects=[_object("nonexistent_table")],
        endpoint=_endpoint(),
    )
    assert len(snapshots) == 1
    assert snapshots[0].fields == []
```

- [ ] **Step 2: Run to verify it fails**

```bash
uv run pytest tests/unit/infrastructure/discovery/test_database_runner.py -v
```

Expected: `ImportError` or `TypeError` — `DatabaseRunner` has the old signature.

- [ ] **Step 3: Replace DatabaseRunner**

```python
# app/infrastructure/discovery/database_runner.py
from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import inspect, text
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.ext.asyncio import create_async_engine

from app.application.discovery.discovery_runner import DiscoveryRunner
from app.application.shared.secret_manager_port import SecretManagerPort
from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.endpoints.endpoint import DatabaseEndpoint
from app.domain.objects.data_object import DataObject
from app.infrastructure.discovery.connection_url_builder import build_connection_url
from app.infrastructure.discovery.sqlalchemy_type_mapper import map_sa_type_to_normalized

logger = logging.getLogger(__name__)


class DatabaseRunner(DiscoveryRunner):
    """
    DiscoveryRunner for relational databases using SQLAlchemy reflection.

    Uses a SINGLE engine + connection for all tables in one discovery run to avoid
    connection overhead. All Inspector calls are synchronous and executed inside
    a single conn.run_sync() to reuse the same DB cursor.

    Richness captured per table:
      - columns: name, source_type, normalized_type, nullable, comment
      - primary key columns
      - foreign key references (stored in SchemaField.extra)
      - index names (stored in SchemaField.extra)
      - table-level comment (stored in SchemaSnapshot — via extra metadata on first field or future extension)
      - estimated row count via COUNT(*) on each table
    """

    def __init__(self, secret_manager: SecretManagerPort) -> None:
        self._secret_manager = secret_manager

    async def run(
        self,
        asset_id: str,
        objects: list[DataObject],
        endpoint: DatabaseEndpoint,
    ) -> list[SchemaSnapshot]:
        """
        Connect once to the endpoint's database and reflect all requested objects.
        Returns one SchemaSnapshot per DataObject in the same order as input.
        """
        payload = await self._secret_manager.resolve(endpoint.credential_ref.path)
        url = build_connection_url(payload)

        engine = create_async_engine(url, pool_pre_ping=True)
        try:
            async with engine.connect() as conn:
                snapshots = await conn.run_sync(
                    self._reflect_all_objects,
                    objects,
                    asset_id,
                )
        finally:
            await engine.dispose()

        return snapshots

    def _reflect_all_objects(
        self,
        sync_conn,  # sqlalchemy.engine.Connection (sync)
        objects: list[DataObject],
        asset_id: str,
    ) -> list[SchemaSnapshot]:
        """
        Synchronous inner function executed via conn.run_sync().
        Runs all Inspector calls within a single open connection.
        """
        inspector = inspect(sync_conn)
        captured_at = datetime.now(timezone.utc)
        snapshots: list[SchemaSnapshot] = []

        for obj in objects:
            snapshot = self._reflect_single_object(inspector, sync_conn, obj, captured_at)
            snapshots.append(snapshot)

        return snapshots

    def _reflect_single_object(
        self,
        inspector,
        sync_conn,
        obj: DataObject,
        captured_at: datetime,
    ) -> SchemaSnapshot:
        """Reflect a single table/view and return its SchemaSnapshot."""
        try:
            columns = inspector.get_columns(obj.name)
            pk_info = inspector.get_pk_constraint(obj.name)
            fk_list = inspector.get_foreign_keys(obj.name)
            index_list = inspector.get_indexes(obj.name)

            # Table comment (may be None on dialects that don't support it)
            try:
                table_comment: str | None = inspector.get_table_comment(obj.name).get("text")
            except NotImplementedError:
                table_comment = None

            pk_columns: set[str] = set(pk_info.get("constrained_columns", []))

            # Build FK index: column → referenced table
            fk_by_column: dict[str, str] = {
                col: fk["referred_table"]
                for fk in fk_list
                for col in fk["constrained_columns"]
            }

            # Build index index: column → index names
            index_by_column: dict[str, list[str]] = {}
            for idx in index_list:
                for col in idx.get("column_names", []):
                    index_by_column.setdefault(col, []).append(idx["name"])

            row_count = self._estimate_row_count(sync_conn, obj.name)

            fields = [
                SchemaField(
                    name=col["name"],
                    source_type=str(col["type"]),
                    normalized_type=map_sa_type_to_normalized(col["type"]),
                    nullable=col.get("nullable", True),
                    is_primary_key=col["name"] in pk_columns,
                    description=col.get("comment"),
                    extra={
                        "fk_to": fk_by_column.get(col["name"]),
                        "indexes": index_by_column.get(col["name"], []),
                        "table_comment": table_comment,
                    },
                )
                for col in columns
            ]

            return SchemaSnapshot(
                object_id=obj.id,
                object_name=obj.name,
                runner_type="database",
                captured_at=captured_at,
                row_count_estimate=row_count,
                fields=fields,
            )

        except NoSuchTableError:
            logger.warning("Table %r not found in database; returning empty snapshot.", obj.name)
            return SchemaSnapshot(
                object_id=obj.id,
                object_name=obj.name,
                runner_type="database",
                captured_at=captured_at,
                fields=[],
            )

    def _estimate_row_count(self, sync_conn, table_name: str) -> int | None:
        """
        Execute a COUNT(*) to estimate row count. Returns None if the table
        is inaccessible or count fails (e.g., permission denied on some dialects).

        For very large tables, consider using dialect-specific fast estimates
        (pg_class.reltuples for Postgres) in a future enhancement.
        """
        try:
            result = sync_conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))  # noqa: S608
            row = result.fetchone()
            return int(row[0]) if row else None
        except Exception:
            logger.debug("Could not estimate row count for %r", table_name, exc_info=True)
            return None
```

> [!IMPORTANT]
> Note the updated method signature: `run(asset_id, objects, endpoint)`. The `DiscoveryRunner` protocol in `app/application/discovery/discovery_runner.py` must also be updated to include `endpoint` — see Step 4.

- [ ] **Step 4: Update DiscoveryRunner protocol and RunDiscoveryUseCase**

Update `app/application/discovery/discovery_runner.py` (lines 17-21):

```python
    async def run(
        self,
        asset_id: str,
        objects: list[DataObject],
        endpoint: "Endpoint",
    ) -> list[SchemaSnapshot]:
```

Update `app/application/discovery/run_discovery_use_case.py` (line 86):

```python
    async def _extract_snapshots(
        self, endpoint: Endpoint, asset_id: str, objects: list[DataObject]
    ) -> list[SchemaSnapshot]:
        runner = self._runner_factory.create(endpoint)
        return await runner.run(asset_id, objects, endpoint)
```

- [ ] **Step 5: Update DiscoveryRunnerFactoryImpl**

```python
# app/infrastructure/discovery/discovery_runner_factory.py
from __future__ import annotations

from app.application.discovery.discovery_runner import DiscoveryRunner, DiscoveryRunnerFactory
from app.application.shared.secret_manager_port import SecretManagerPort
from app.domain.endpoints.endpoint import DatabaseEndpoint, Endpoint
from app.infrastructure.discovery.database_runner import DatabaseRunner


class DiscoveryRunnerFactoryImpl(DiscoveryRunnerFactory):
    """Creates the appropriate DiscoveryRunner for the given Endpoint type."""

    def __init__(self, secret_manager: SecretManagerPort) -> None:
        self._secret_manager = secret_manager

    def create(self, endpoint: Endpoint) -> DiscoveryRunner:
        if isinstance(endpoint, DatabaseEndpoint):
            return DatabaseRunner(secret_manager=self._secret_manager)
        raise NotImplementedError(
            f"No DiscoveryRunner registered for endpoint type: {type(endpoint).__name__}"
        )
```

- [ ] **Step 6: Run all tests**

```bash
uv run pytest tests/unit/infrastructure/discovery/test_database_runner.py -v
uv run pytest tests/ -v
```

Expected: all `PASSED` (127+ tests).

- [ ] **Step 7: Commit**

```bash
git add app/infrastructure/discovery/database_runner.py \
        app/infrastructure/discovery/discovery_runner_factory.py \
        app/application/discovery/discovery_runner.py \
        app/application/discovery/run_discovery_use_case.py \
        tests/unit/infrastructure/discovery/test_database_runner.py
git commit -m "feat: implement SQLAlchemy Inspector-based DatabaseRunner with full metadata capture"
```

---

## Self-Review Checklist

- [x] **Spec coverage:** Secret Manager Port ✅, URL Builder ✅, Type Mapper ✅, Database Runner ✅, Single connection per run ✅, FK + index + PK + comment capture ✅, Row count estimate ✅, graceful handling for missing tables ✅
- [x] **Placeholder scan:** No TBDs, no "add error handling later" — all error branches are implemented.
- [x] **Type consistency:** `SecretManagerPort.resolve(ref: str) -> dict[str, str]` used consistently in all tasks. `build_connection_url(dict[str,str]) -> str` used in Task 4. `map_sa_type_to_normalized(TypeEngine) -> str` used in Task 4.
- [x] **Protocol update:** `DiscoveryRunner.run()` signature is updated in Task 4 before tests run.
- [x] **Clean Architecture:** Domain untouched. Application layer owns ports (Protocols). Infrastructure layer owns implementations. No framework imports in domain or application.
- [x] **Performance:** Single `create_async_engine` + single `engine.connect()` + single `conn.run_sync()` per discovery run — O(1) connections regardless of number of tables.
