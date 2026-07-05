# Operations Guide

This guide provides step-by-step instructions on how to operate the Airflow 3 Data Platform locally, simulate its lifecycle, connect to local services, and test the use cases.

---

## 1. Environment Setup

### Prerequisites
- Docker Desktop (with WSL2 enabled if on Windows)
- `uv` (Fast Python package manager)
- Git

### Bootstrapping the Cluster
To start the local simulation with Airflow, PostgreSQL, OpenBao (Vault), and our FastAPI platform:

```bash
# 1. Start the Docker containers in the background and build images
docker compose up -d --build

# 2. Wait for initialization
# The `airflow-init` container will run database migrations and create the Admin user.
# You can check its logs:
docker compose logs -f airflow-init

# 3. Access Airflow UI
# Navigate to http://localhost:8080
# Username: admin
# Password: admin
```

### Initializing the Platform Database (FastAPI)
The `platform_db` is created by Postgres, but you must run the migrations/init scripts to create the database schemas:

```bash
uv run python scripts/init_db.py
```

---

## 2. Docker Container Operations

### Accessing Bash/Shell of Containers
Use the following commands to drop into the shell of any running container:

```bash
# PostgreSQL
docker exec -it airflow-data-platform-sdd-postgres-1 bash

# OpenBao (Vault)
docker exec -it airflow-data-platform-sdd-openbao-1 sh

# Platform API (FastAPI)
docker exec -it airflow-data-platform-sdd-platform-api-1 bash

# Airflow Scheduler
docker exec -it airflow-data-platform-sdd-airflow-scheduler-1 bash
```

---

## 3. Database Operations (PostgreSQL)

Our PostgreSQL container hosts two distinct databases:
1. `platform_db`: Holds platform-specific records (Assets, Endpoints, DataObjects, Discovery Runs, etc.).
2. `airflow`: Holds the Airflow scheduler metadata tables.

### Connecting to Platform Database (`platform_db`)
From your host machine:
```bash
psql -h localhost -p 5432 -U airflow -d platform_db
# (Default password is "airflow")
```

From inside the postgres container:
```bash
docker exec -it airflow-data-platform-sdd-postgres-1 psql -U airflow -d platform_db
```

### Connecting to Airflow Database (`airflow`)
From your host machine:
```bash
psql -h localhost -p 5432 -U airflow -d airflow
# (Default password is "airflow")
```

From inside the postgres container:
```bash
docker exec -it airflow-data-platform-sdd-postgres-1 psql -U airflow -d airflow
```

### Basic SQL Commands & Table Inspection (in `platform_db`)
List all tables:
```sql
\dt
```

Query registered Assets:
```sql
SELECT id, name, state, owner_email FROM data_assets;
```

Query registered Endpoints:
```sql
SELECT id, asset_id, type, credential_ref FROM endpoints;
```

Query Data Objects:
```sql
SELECT id, asset_id, name, type, freshness_status FROM data_objects;
```

Query Discovery Runs & Drift Approvals:
```sql
SELECT id, asset_id, status, started_at, completed_at FROM discovery_runs;
SELECT id, asset_id, object_id, change_type, severity_description, decision FROM drift_approvals;
```

Query Catalog Schema Versions:
```sql
SELECT id, object_id, version, created_at FROM catalog_schema_versions;
```

---

## 4. End-to-End Walkthrough: Credentials, Asset, and Discovery Simulation

This step-by-step example demonstrates registering credentials in OpenBao, creating an Endpoint, creating a Data Asset, activating the Asset by linking the Endpoint, mapping a PostgreSQL table as a DataObject, and triggering Discovery. All API calls now use human-readable names instead of database UUIDs.

### Step 1: Register Postgres Credentials in OpenBao (Vault)
OpenBao is running in dev mode with token `root`. We write postgres login parameters to the KV v2 secrets engine path `secret/postgres`:

**Using curl from host:**
```bash
curl --header "X-Vault-Token: root" \
     --request POST \
     --data '{"data": {"username": "airflow", "password": "airflow", "host": "postgres", "port": "5432", "database": "platform_db"}}' \
     http://localhost:8200/v1/secret/data/postgres
```

**Alternative (using Bao CLI inside container):**
```bash
docker exec -it airflow-data-platform-sdd-openbao-1 sh
# Inside container:
export BAO_ADDR=http://localhost:8200
export BAO_TOKEN=root
bao kv put secret/postgres \
    username=airflow \
    password=airflow \
    host=postgres \
    port=5432 \
    database=platform_db
```

---

### Step 2: Register a Database Endpoint
We register a database endpoint first, making it independent of any specific asset. It references the OpenBao credentials we created in Step 1.

```bash
curl -X POST "http://localhost:8000/endpoints/database" \
     -H "Authorization: Bearer sre" \
     -H "Content-Type: application/json" \
     -d '{
       "name": "sales-db-prod",
       "credential_ref": "secret/postgres",
       "technical_description": "Production database for sales data"
     }'
```

---

### Step 3: Register a Data Asset (DRAFT state)
We register a new DataAsset using a PO/PM role bearer token (`po_pm` token bypasses authentication checks to mock PO_PM permissions):

```bash
curl -X POST "http://localhost:8000/assets/" \
     -H "Authorization: Bearer po_pm" \
     -H "Content-Type: application/json" \
     -d '{
       "name": "sales-database-asset",
       "description": "Sales transaction data source",
       "owner_email": "sales-owner@company.com",
       "tags": ["sales", "postgres"],
       "policy_tags": [],
       "discovery_schedule": "0 0 * * *",
       "discovery_scope_include": ["*"],
       "discovery_scope_exclude": []
     }'
```

---

### Step 4: Activate the Data Asset
To link the endpoint to the asset and transition the asset from `DRAFT` to `ACTIVE` (requires SRE role token `sre`):

```bash
curl -X POST "http://localhost:8000/assets/sales-database-asset/activate?endpoint_name=sales-db-prod" \
     -H "Authorization: Bearer sre"
```

---

### Step 5: Update the Data Asset Metadata (Optional)
If you need to update the asset's description, tags, or even link a different endpoint, you can use the PUT endpoint:

```bash
curl -X PUT "http://localhost:8000/assets/sales-database-asset" \
     -H "Authorization: Bearer po_pm" \
     -H "Content-Type: application/json" \
     -d '{
       "description": "Updated Sales transaction data source",
       "tags": ["sales", "postgres", "updated"],
       "policy_tags": [],
       "endpoint_name": "sales-db-prod"
     }'
```

---

### Step 6: Map a Database Table (DataObject)
Currently, data objects are populated from existing mappings. To tell the Discovery runner which database objects (tables/views) to reflect, connect to the postgres database `platform_db` and insert a record representing the `pipelines` table (which is already created by SQLAlchemy):

```bash
docker exec -it airflow-data-platform-sdd-postgres-1 psql -U airflow -d platform_db
```
Then run the SQL query (we dynamically resolve the `asset_id` using the asset name):
```sql
INSERT INTO data_objects (id, asset_id, name, type, description, policy_tags, elements, created_at, updated_at, freshness_status, auto_generated_description)
VALUES (
  'obj-sales-pipelines',
  (SELECT id FROM data_assets WHERE name = 'sales-database-asset'),
  'pipelines',
  'TABLE',
  'Sales pipeline catalog mapping',
  '[]',
  '[]',
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP,
  'unknown',
  false
);
```

---

### Step 7: Trigger Discovery and Validate
Trigger a manual discovery run for the asset using its name:

```bash
curl -X POST "http://localhost:8000/discovery/assets/sales-database-asset/run" \
     -H "Authorization: Bearer po_pm" \
     -H "Content-Type: application/json" \
     -d '{"triggered_by": "manual-operator"}'
```

#### Validations:
1. **Metadata & Self-Healing**: Query the `data_objects` table to verify the columns of the `pipelines` table were reflected, parsed, and populated into the JSON list of `elements`:
   ```sql
   SELECT name, elements FROM data_objects WHERE name = 'pipelines';
   ```
2. **Schema Versioning & Catalog History**: Verify that a new version of the metadata schema was written to the catalog versions database log:
   ```sql
   SELECT * FROM catalog_schema_versions;
   ```

---

## 5. Pipeline Lifecycle Simulation (YAML & CLI)

### Step A: Managing YAML Configurations
All pipelines are declared via YAML in the `dags/yamls/` folder (or pulled from a central Git repo).
1. Create a file `dags/yamls/my_pipeline.yaml`.
2. Define the pipeline structure (see examples).

### Step B: Rebuilding DAGs via Typer CLI
Instead of Airflow parsing YAMLs dynamically (which causes scheduler overload), we use a decoupled Typer CLI to generate `.py` files.

```bash
# Run the CLI to validate YAMLs and generate Python DAGs
uv run cli pipeline rebuild dags/

# For a dry-run to see diffs:
uv run cli pipeline rebuild dags/ --dry-run
```
The generated `.py` files will appear in the `dags/` folder and Airflow will instantly pick them up.

---

## 6. Platform Evolution & Refactoring

When adding new features (e.g., a new integration or use case):
1. **Domain First:** Create the models in `app/domain/`. Ensure they use `@dataclass(kw_only=True)`.
2. **Use Case:** Implement business logic in `app/application/`. Rely on abstract protocols (like `UnitOfWork`).
3. **Infrastructure:** Add SQLAlchemy mappers and repositories in `app/infrastructure/`.
4. **Testing:** Run the full suite before committing:
   ```bash
   uv run pytest tests/ -v
   make type-check
   ```
