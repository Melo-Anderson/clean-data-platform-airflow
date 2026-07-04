# Operations Guide

This guide provides step-by-step instructions on how to operate the Airflow 3 Data Platform locally, simulate its lifecycle, and evolve its architecture.

## 1. Environment Setup

### Prerequisites
- Docker Desktop (with WSL2 enabled if on Windows)
- `uv` (Fast Python package manager)
- Git

### Bootstrapping the Cluster
To start the local simulation with Airflow, PostgreSQL, and our FastAPI platform:

```bash
# 1. Start the Docker containers in the background
docker compose up -d

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
The `platform_db` is created by Postgres, but you must run the Alembic migrations or create the tables. For development, the API handles table creation on startup, but you can also execute:

```bash
uv run python scripts/init_db.py
```

## 2. Platform Lifecycle Simulation

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

### Step C: Triggering Discovery
Simulate a metadata discovery event (e.g., detecting a schema drift):

```bash
# Using the CLI
uv run cli discovery run --asset-id "asset-123"

# Approving a schema drift
uv run cli discovery approve "drift-approval-uuid" --user "admin@example.com"
```

### Step D: Hitting the FastAPI Endpoints
The platform exposes APIs for lineage, metadata, and assets.
- Swagger UI: `http://localhost:8000/docs`
- Example Request:
  ```bash
  curl -X GET "http://localhost:8000/lineage/trace?object_id=obj_1&column_name=col_a&direction=upstream"
  ```

## 3. Platform Evolution & Refactoring

When adding new features (e.g., a new integration or use case):
1. **Domain First:** Create the models in `app/domain/`. Ensure they use `@dataclass(kw_only=True)`.
2. **Use Case:** Implement business logic in `app/application/`. Rely on abstract protocols (like `UnitOfWork`).
3. **Infrastructure:** Add SQLAlchemy mappers and repositories in `app/infrastructure/`.
4. **Testing:** Run the full suite before committing:
   ```bash
   uv run pytest tests/ -v
   make type-check
   ```
