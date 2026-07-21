# E2E & Performance Testing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement granular Docker Compose profiles, data seed scripts for MongoDB and Postgres, and E2E performance tests for the Discovery engine.

**Architecture:**
1. `docker-compose.yml` updated with `profiles` (`core`, `api`, `airflow`, `e2e-mongo`, `e2e-pg-perf`, `e2e-runner`).
2. Data seeding scripts centralized in `scripts/e2e_seeds/` and mapped to entrypoints.
3. Pytest E2E tests for MongoDB (hybrid discovery) and Postgres (structural performance).

**Tech Stack:** Docker Compose, Pytest, PostgreSQL (PL/pgSQL), MongoDB (JS).

## Global Constraints

- No modifications to the application domain code; this is purely infrastructure and E2E testing.
- `docker-compose.yml` must use valid Compose V2 `profiles` syntax.
- All seed scripts must be idempotent.

---

### Task 1: Update Docker Compose Profiles and Add New Services

**Files:**
- Modify: `docker-compose.yml`

**Interfaces:**
- Produces: `e2e-mongo` service, `postgres-perf` service. Profile groupings for all services.

- [ ] **Step 1: Write the failing test**

*(We skip TDD for docker-compose configuration as it is declarative infrastructure)*

- [ ] **Step 2: Write minimal implementation**

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

# Add new service:
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
    volumes:
      - ./scripts/e2e_seeds/postgres_perf_schema.sql:/docker-entrypoint-initdb.d/postgres_perf_schema.sql

# Update e2e-tests:
#     profiles:
#       - e2e-runner
```
*(Agent: Apply these changes carefully, respecting the existing structure of docker-compose.yml)*

- [ ] **Step 3: Run test to verify it passes**

Run: `docker compose config`
Expected: Valid compose file output.

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml
git commit -m "test(infrastructure): add docker compose profiles and e2e databases"
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
import pytest
import httpx
import os
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

pytestmark = pytest.mark.e2e

API_URL = os.getenv("API_URL", "http://platform-api:8000")
PLATFORM_DATABASE_URL = os.getenv("PLATFORM_DATABASE_URL", "postgresql+asyncpg://airflow:airflow@postgres:5432/platform_db")

@pytest.mark.asyncio
async def test_mongo_discovery_e2e(api_client: httpx.AsyncClient, sre_client: httpx.AsyncClient) -> None:
    # Register Endpoint
    await sre_client.post("/v1/endpoints/nosql", json={
        "name": "e2e-mongo",
        "credential_ref": "secret/mongo",
        "technical_description": "Mongo DB"
    })

    # Register Asset
    resp = await api_client.post("/v1/assets/", json={
        "name": "e2e-mongo-asset",
        "description": "Mongo E2E",
        "owner_email": "e2e@co.com",
        "tags": ["mongo"],
        "policy_tags": [],
        "discovery_schedule": "0 0 * * *",
        "discovery_scope_include": ["test_db.*"],
        "discovery_scope_exclude": []
    })

    # Activate
    await sre_client.post("/v1/assets/e2e-mongo-asset/activate", params={"endpoint_name": "e2e-mongo"})

    # Trigger Discovery
    resp = await api_client.post("/v1/discovery/assets/e2e-mongo-asset/run", json={"triggered_by": "e2e_test"})
    assert resp.status_code == 201

    # In a real scenario we'd wait for completion via API polling or checking status.
    # We will assume it runs synchronously for testing or we poll the DB for DataObjects

    engine = create_async_engine(PLATFORM_DATABASE_URL)
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    import asyncio

    # Poll database for extracted metadata
    for _ in range(10):
        async with async_session() as session:
            result = await session.execute(
                text("SELECT name FROM data_objects WHERE name LIKE 'test_db.%'")
            )
            rows = result.fetchall()
            names = [r[0] for r in rows]
            if "test_db.users_strict" in names and "test_db.logs_loose" in names:
                break
        await asyncio.sleep(2)

    assert "test_db.users_strict" in names
    assert "test_db.logs_loose" in names

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
import pytest
import httpx
import os
import time
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

pytestmark = pytest.mark.e2e

API_URL = os.getenv("API_URL", "http://platform-api:8000")
PLATFORM_DATABASE_URL = os.getenv("PLATFORM_DATABASE_URL", "postgresql+asyncpg://airflow:airflow@postgres:5432/platform_db")

@pytest.mark.asyncio
async def test_postgres_perf_e2e(api_client: httpx.AsyncClient, sre_client: httpx.AsyncClient) -> None:
    # Register Endpoint
    await sre_client.post("/v1/endpoints/database", json={
        "name": "e2e-pg-perf",
        "credential_ref": "secret/pg_perf",
        "technical_description": "Perf DB"
    })

    # Register Asset
    await api_client.post("/v1/assets/", json={
        "name": "e2e-pg-perf-asset",
        "description": "Postgres Perf",
        "owner_email": "e2e@co.com",
        "tags": ["perf"],
        "policy_tags": [],
        "discovery_schedule": "0 0 * * *",
        "discovery_scope_include": ["public.*"],
        "discovery_scope_exclude": []
    })

    # Activate
    await sre_client.post("/v1/assets/e2e-pg-perf-asset/activate", params={"endpoint_name": "e2e-pg-perf"})

    # Trigger Discovery
    start_time = time.time()
    resp = await api_client.post("/v1/discovery/assets/e2e-pg-perf-asset/run", json={"triggered_by": "perf_test"})
    assert resp.status_code == 201

    engine = create_async_engine(PLATFORM_DATABASE_URL)
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    import asyncio

    found_count = 0
    # Poll database
    for _ in range(15):
        async with async_session() as session:
            result = await session.execute(
                text("SELECT count(*) FROM data_objects WHERE name LIKE 'public.synthetic_table_%'")
            )
            found_count = result.scalar()
            if found_count >= 300:
                break
        await asyncio.sleep(2)

    end_time = time.time()

    assert found_count >= 300, f"Expected 300+ synthetic tables, found {found_count}"

    # Validate edge cases
    async with async_session() as session:
        result = await session.execute(
            text("SELECT name FROM data_objects WHERE name = 'public.edge_case_table'")
        )
        assert result.fetchone() is not None, "Edge case table missing"

    # Performance validation (less than 30 seconds local extraction)
    assert (end_time - start_time) < 30.0, f"Discovery took too long: {end_time - start_time}s"

    await engine.dispose()
```

- [ ] **Step 2: Commit**

```bash
git add tests/e2e/test_postgres_perf_e2e.py
git commit -m "test(e2e): implement postgres structural performance test"
```
