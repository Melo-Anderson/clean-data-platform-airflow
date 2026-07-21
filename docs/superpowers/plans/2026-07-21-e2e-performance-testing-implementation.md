# E2E & Performance Testing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement granular Docker Compose profiles, data seed scripts for MongoDB and Postgres, and E2E performance tests for the Discovery engine.

**Architecture:**
1. `docker-compose.yml` updated with `profiles` (`core`, `api`, `airflow`, `e2e-mongo`, `e2e-pg-perf`, `e2e-runner`).
2. Data seeding scripts centralized in `scripts/e2e_seeds/` and mounted via Docker volumes at `/docker-entrypoint-initdb.d/`.
3. Pytest E2E tests share fixtures (`api_client`, `sre_client`) from the existing `conftest.py` in `tests/e2e/`.

**Tech Stack:** Docker Compose, Pytest, PostgreSQL (PL/pgSQL), MongoDB (JS), SQLAlchemy async.

## Global Constraints

- No modifications to the application domain code; this is purely infrastructure and E2E testing.
- `docker-compose.yml` must use valid Compose V2 `profiles` syntax.
- All seed scripts must be idempotent (use `IF NOT EXISTS` / `db.getCollectionNames()` guards).
- The `api_client` and `sre_client` fixtures already exist in `tests/e2e/test_platform_e2e.py`. Move them to `tests/e2e/conftest.py` in Task 1 so all new test files can share them without duplication.
- Use `time.monotonic()` instead of `time.time()` for measuring elapsed wall-clock time in performance tests.
- All imports must be at the top of the file — never inside function bodies (platform rule).
- `names` variable scope: always initialise to `[]` before the polling loop to prevent `UnboundLocalError` if the loop exits without breaking.

---

### Task 1: Shared Fixtures + Docker Compose Profiles + New Services

**Files:**
- Create: `tests/e2e/conftest.py`
- Modify: `tests/e2e/test_platform_e2e.py` (remove duplicated fixtures)
- Modify: `docker-compose.yml`

**Interfaces:**
- Produces: `api_client` / `sre_client` fixtures available project-wide via `conftest.py`; `e2e-mongo` and `postgres-perf` services; profile groupings for all existing services.

- [ ] **Step 1: Extract shared fixtures into conftest.py**

Create `tests/e2e/conftest.py` with the fixtures that are currently duplicated inside `test_platform_e2e.py`:

```python
from __future__ import annotations

import time
import os

import httpx
import jwt as pyjwt
import pytest

API_URL = os.getenv("API_URL", "http://platform-api:8000")

PRIVATE_KEY_PEM = """-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCp17PsSTf3e03m
wR76GCgm3zpASYab1XkGJirst/NZvQZ88A1u2QTiQeWhO7TDLXinko2n0ZFxNZSX
2/wQcBMKCnwWxq/xFE6b73zHQkoduj+YQj2f+8xvY+Iq0oEyIi6DKKFm27jsd+uY
CYauZnr9dKKbv7ruv+L0KgwosCxqrCsxNhDZl/08/lSb2LXfIybJuh6VMQBRLqkT
15pDIybwSGCjy4BgIyUEqwjOc+AcoYDMv0107TWMu4IaCvgiUPZihzZZsqAV090l
yiuyF53+rv84oLL+zHy/NG7Mpii7vJnTaUPf9bBFW7MLwjwdlkh4ov4/MSJqsITy
Y+oJG3adAgMBAAECggEABDMZt1N+J0fsvrJyxiNXxtJJOfK3ed327qB9+jl4MnVa
ljdHVcDW/pM7jtePmi3jKF2W1Bn5+y8ke/bMDkn/JoXo2JVUH2VtpixvTOwGMiL7
VJP6uxx6SxzQqFdpK2it9r9H8mendG1orWs64dAV5XN/W9OLV0D2Zyws/cqRZpfN
5aZyf1871UvHQgK49kjWQ69ipGZM92bc/vESGxpAZeKKYSYXtkkWxMzpAR7SeSZ5
zIQrd5cX94OzKhoGqAGQUTWTetfBTIsczRu0K+bDBwwE59nMtUQ3M5F5ic3fEQMR
WdF6cowUPB8yHFHsEVY3boA9VATO3EQxnDLENCzCrwKBgQDjj2/7e32EaH7HUkUv
p3hEeztKgf/1N7JvIlo5Sa11v50QKhwAicKYgaLfTmddtzXdrnt8cZQ+OGnR+qGn
90IaY1zcnYEHk6UTldN6h3v0aFQTUzMG2OcAgJsV66hzxg1DyMpnG1Fa5XAmRZll
1rbOMJz2Ck9B5LU3ZkRvygXjDwKBgQC/EaUzfZVED7i7DgW+xY/IjZVJzQ8tvkfz
1TOYtmvlxkg4v8CVLvQ/b+N2qqaZn3wTH9mAU0YUOM4Q1dfvPrD4d+A63Rg32+1U
tEwc46/5PMaCtGxmO7WLccFgk1wyaTkc30h8jofuqJmaR0y3HVv/0M29meLsR+N3
0q3AFMCbkwKBgQDDGvJKTiDZ67X3M4R6TT4CiR3WzgsktjJYsr1krNT6ReVmPJRx
qaucklmQ2Goroa+fd8AMfF0706Z3EEqV9ptIgLTXunssgdxhJG6DebI/ZUvgnc78
KfA1MA7IBpsRWFd7LKbNLFDefCVhyv6woB1wP6H0GfbGak8tRpOavT265QKBgGj1
Z3umk/WEcWUH6e4HFtoDtKuK4ritG1d9mc9c/l6Fkqzh4QfSeEfUze4lBknDi2Py
DgfpNsjq/3/OCMWa+Zo0N8/+HkypGnF6bYk9JjDSyvWH6Tgruqm0Ppcvu+jRVpde
rLIHlfJrWZ2fZyv8C8q2SB7MRxSm1PTAncOzYq7TAoGBAODoOW0Knt4TdFh3cdbF
GFWEULjJG5Y5AasIKRn8QpjCOaKVwib78gJZtj9DalUFiJ6pYsTd4YibB5/2XVLm
UHROCgh5z7TbPnCEobz5nLv0Z3ZGuAZJiUD4mNNAKhtLE0BXpzSQBy9wl2a56HCZ
nqPPnQGKt6gwFDkPJwzkr4lY
-----END PRIVATE KEY-----"""


def _get_token(role: str) -> str:
    payload = {
        "sub": "u1_e2e",
        "email": f"{role}_e2e@co.com",
        "roles": [role],
        "exp": int(time.time()) + 3600,
    }
    return pyjwt.encode(payload, PRIVATE_KEY_PEM, algorithm="RS256")


@pytest.fixture
async def api_client() -> httpx.AsyncClient:
    token = _get_token("analytics_engineer")
    async with httpx.AsyncClient(
        base_url=API_URL, headers={"Authorization": f"Bearer {token}"}, timeout=60.0
    ) as client:
        yield client


@pytest.fixture
async def sre_client() -> httpx.AsyncClient:
    token = _get_token("sre")
    async with httpx.AsyncClient(
        base_url=API_URL, headers={"Authorization": f"Bearer {token}"}, timeout=60.0
    ) as client:
        yield client
```

Then remove the duplicated `PRIVATE_KEY_PEM`, `_get_token`, `api_client`, and `sre_client` definitions from `tests/e2e/test_platform_e2e.py`.

- [ ] **Step 2: Update docker-compose.yml**

Update `docker-compose.yml` to inject profiles into existing services and add the new test databases.

```yaml
# Add to postgres, openbao, openbao-init:
#     profiles:
#       - core

# Add to airflow-webserver, airflow-scheduler, airflow-init:
#     profiles:
#       - airflow

# Add to platform-api:
#     profiles:
#       - api

# Add new services:
  mongodb:
    image: mongo:6.0
    profiles:
      - e2e-mongo
    ports:
      - "27017:27017"
    volumes:
      - ./scripts/e2e_seeds/mongo_init_schema.js:/docker-entrypoint-initdb.d/mongo_init_schema.js

  postgres-perf:
    image: postgres:15
    profiles:
      - e2e-pg-perf
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: perf_db
    ports:
      - "5433:5432"
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      retries: 5
      start_period: 5s
    volumes:
      - ./scripts/e2e_seeds/postgres_perf_schema.sql:/docker-entrypoint-initdb.d/postgres_perf_schema.sql

# Update e2e-tests:
#     profiles:
#       - e2e-runner
```
*(Agent: Apply these changes carefully, respecting the existing indentation and structure of docker-compose.yml. Add `healthcheck` to `postgres-perf` mirroring the existing `postgres` service.)*

- [ ] **Step 3: Verify compose config is valid**

Run: `docker compose config`
Expected: Valid compose file output, no errors.

- [ ] **Step 4: Commit**

```bash
git add tests/e2e/conftest.py tests/e2e/test_platform_e2e.py docker-compose.yml
git commit -m "test(infrastructure): extract shared fixtures and add docker compose profiles"
```

---

### Task 2: Create MongoDB Seed Script

**Files:**
- Create: `scripts/e2e_seeds/mongo_init_schema.js`

**Interfaces:**
- Consumes: Nothing
- Produces: Initialized MongoDB database `test_db` with `users_strict` and `logs_loose` collections.

- [ ] **Step 1: Write minimal implementation**

```javascript
db = db.getSiblingDB('test_db');

// Collection 1: Strict JSON Schema
db.createCollection("users_strict", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["name", "email", "age"],
            properties: {
                name: { bsonType: "string", description: "must be a string and is required" },
                email: { bsonType: "string", description: "must be a string and is required" },
                age: { bsonType: "int", minimum: 0, description: "must be an integer and is required" }
            }
        }
    }
});

db.users_strict.insertMany([
    { name: "Alice", email: "alice@test.com", age: NumberInt(30) },
    { name: "Bob", email: "bob@test.com", age: NumberInt(25) }
]);

// Collection 2: Loose / Schemaless
db.createCollection("logs_loose");

db.logs_loose.insertMany([
    { level: "INFO", message: "Server started", timestamp: new Date() },
    { level: "ERROR", message: "Connection lost", code: 500 },
    { user_id: 123, action: "login", success: true }
]);
```

- [ ] **Step 2: Commit**

```bash
git add scripts/e2e_seeds/mongo_init_schema.js
git commit -m "test(e2e): add mongodb seed script with strict and loose collections"
```

---

### Task 3: Create Postgres Performance Seed Script

**Files:**
- Create: `scripts/e2e_seeds/postgres_perf_schema.sql`

**Interfaces:**
- Consumes: Nothing
- Produces: 300 tables, views, indices, and exotic data types in `perf_db`.

- [ ] **Step 1: Write minimal implementation**

```sql
DO $$
DECLARE
    i INT;
BEGIN
    -- Create 300 standard tables
    FOR i IN 1..300 LOOP
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS synthetic_table_%s (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) DEFAULT ''active'',
                value NUMERIC(10, 2)
            );
            CREATE INDEX IF NOT EXISTS idx_synthetic_%s_status ON synthetic_table_%s(status);
            COMMENT ON TABLE synthetic_table_%s IS ''Autogenerated synthetic table %s'';
            COMMENT ON COLUMN synthetic_table_%s.name IS ''Name of the entity'';
        ', i, i, i, i, i, i);
    END LOOP;

    -- Create Edge Case Table with exotic types
    CREATE TABLE IF NOT EXISTS edge_case_table (
        id UUID PRIMARY KEY,
        metadata JSONB,
        ip_addr INET,
        network CIDR,
        mac MACADDR,
        search_vector TSVECTOR,
        coordinates POINT,
        flags BOOLEAN[]
    );
    COMMENT ON TABLE edge_case_table IS 'Table containing rare postgres types';

    -- Create Foreign Keys
    CREATE TABLE IF NOT EXISTS synthetic_child (
        id SERIAL PRIMARY KEY,
        parent_id INT REFERENCES synthetic_table_1(id)
    );

    -- Create Views
    CREATE OR REPLACE VIEW active_synthetic_1 AS
        SELECT * FROM synthetic_table_1 WHERE status = 'active';

END $$;
```

- [ ] **Step 2: Commit**

```bash
git add scripts/e2e_seeds/postgres_perf_schema.sql
git commit -m "test(e2e): add massive postgres schema seed script for performance testing"
```

---

### Task 4: Implement MongoDB E2E Test

**Files:**
- Create: `tests/e2e/test_mongo_discovery_e2e.py`

**Interfaces:**
- Consumes: `mongodb` service (`test_db`)
- Produces: Passing Pytest for Mongo Discovery.

- [ ] **Step 1: Write the failing test & minimal implementation**

```python
from __future__ import annotations

import asyncio
import os

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.e2e

PLATFORM_DATABASE_URL = os.getenv(
    "PLATFORM_DATABASE_URL", "postgresql+asyncpg://airflow:airflow@postgres:5432/platform_db"
)


@pytest.mark.asyncio
async def test_mongo_discovery_e2e(
    api_client: httpx.AsyncClient, sre_client: httpx.AsyncClient
) -> None:
    # Register Endpoint — idempotent (409 accepted if already registered)
    await sre_client.post("/v1/endpoints/nosql", json={
        "name": "e2e-mongo",
        "credential_ref": "secret/mongo",
        "technical_description": "MongoDB E2E test database",
    })

    # Register Asset — idempotent (409 accepted)
    await api_client.post("/v1/assets/", json={
        "name": "e2e-mongo-asset",
        "description": "MongoDB E2E data asset for hybrid discovery testing",
        "owner_email": "e2e@co.com",
        "tags": ["mongo", "e2e"],
        "policy_tags": [],
        "discovery_schedule": "0 0 * * *",
        "discovery_scope_include": ["test_db.*"],
        "discovery_scope_exclude": [],
    })

    # Activate (SRE role required — see business_rules.md Fluxo A)
    await sre_client.post("/v1/assets/e2e-mongo-asset/activate", params={"endpoint_name": "e2e-mongo"})

    # Trigger Discovery and assert that the run was accepted
    resp = await api_client.post(
        "/v1/discovery/assets/e2e-mongo-asset/run", json={"triggered_by": "e2e_test"}
    )
    assert resp.status_code == 201

    engine = create_async_engine(PLATFORM_DATABASE_URL)
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    # Initialise before loop to guarantee variable is always bound
    names: list[str] = []

    # Poll platform metadata DB until both DataObjects are persisted (max 20s)
    for _ in range(10):
        async with async_session() as session:
            result = await session.execute(
                text("SELECT name FROM data_objects WHERE name LIKE 'test_db.%'")
            )
            names = [row[0] for row in result.fetchall()]
            if "test_db.users_strict" in names and "test_db.logs_loose" in names:
                break
        await asyncio.sleep(2)

    assert "test_db.users_strict" in names, f"users_strict not discovered. Found: {names}"
    assert "test_db.logs_loose" in names, f"logs_loose not discovered. Found: {names}"

    await engine.dispose()
```

- [ ] **Step 2: Commit**

```bash
git add tests/e2e/test_mongo_discovery_e2e.py
git commit -m "test(e2e): implement mongodb hybrid discovery test"
```

---

### Task 5: Implement Postgres Performance E2E Test

**Files:**
- Create: `tests/e2e/test_postgres_perf_e2e.py`

**Interfaces:**
- Consumes: `postgres-perf` service (`perf_db`)
- Produces: Passing Pytest for Postgres Performance.

- [ ] **Step 1: Write the failing test & minimal implementation**

```python
from __future__ import annotations

import asyncio
import os
import time

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.e2e

PLATFORM_DATABASE_URL = os.getenv(
    "PLATFORM_DATABASE_URL", "postgresql+asyncpg://airflow:airflow@postgres:5432/platform_db"
)
# Connection URL targeting the isolated perf database on port 5433
PERF_DATABASE_URL = os.getenv(
    "PERF_DATABASE_URL", "postgresql+asyncpg://airflow:airflow@postgres-perf:5432/perf_db"
)


@pytest.mark.asyncio
async def test_postgres_perf_e2e(
    api_client: httpx.AsyncClient, sre_client: httpx.AsyncClient
) -> None:
    # Register Endpoint — points to the postgres-perf container (credential seeded in openbao-init)
    await sre_client.post("/v1/endpoints/database", json={
        "name": "e2e-pg-perf",
        "credential_ref": "secret/pg_perf",
        "technical_description": "Isolated Postgres container with 300+ synthetic tables for structural performance testing",
    })

    # Register Asset
    await api_client.post("/v1/assets/", json={
        "name": "e2e-pg-perf-asset",
        "description": "Postgres structural performance asset",
        "owner_email": "e2e@co.com",
        "tags": ["perf", "e2e"],
        "policy_tags": [],
        "discovery_schedule": "0 0 * * *",
        "discovery_scope_include": ["public.*"],
        "discovery_scope_exclude": [],
    })

    # Activate (SRE role required)
    await sre_client.post(
        "/v1/assets/e2e-pg-perf-asset/activate", params={"endpoint_name": "e2e-pg-perf"}
    )

    # Use monotonic clock to avoid wall-clock drift in CI environments
    start_time = time.monotonic()
    resp = await api_client.post(
        "/v1/discovery/assets/e2e-pg-perf-asset/run", json={"triggered_by": "perf_test"}
    )
    assert resp.status_code == 201

    engine = create_async_engine(PLATFORM_DATABASE_URL)
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    found_count = 0
    # Poll platform metadata DB until all 300 synthetic DataObjects are persisted (max 30s)
    for _ in range(15):
        async with async_session() as session:
            result = await session.execute(
                text("SELECT count(*) FROM data_objects WHERE name LIKE 'public.synthetic_table_%'")
            )
            found_count = result.scalar() or 0
            if found_count >= 300:
                break
        await asyncio.sleep(2)

    elapsed = time.monotonic() - start_time

    assert found_count >= 300, f"Expected 300+ synthetic tables in metadata DB, found {found_count}"

    # Validate that the edge case table with exotic types was also discovered
    async with async_session() as session:
        result = await session.execute(
            text("SELECT name FROM data_objects WHERE name = 'public.edge_case_table'")
        )
        assert result.fetchone() is not None, "edge_case_table missing from Discovery results"

    # SLA: full structural discovery of 300 tables must complete under 30 seconds locally
    assert elapsed < 30.0, f"Discovery SLA breached: took {elapsed:.2f}s (limit: 30s)"

    await engine.dispose()
```

- [ ] **Step 2: Commit**

```bash
git add tests/e2e/test_postgres_perf_e2e.py
git commit -m "test(e2e): implement postgres structural performance test"
```
