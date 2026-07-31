# Google BigQuery DWH Loader & Provisioning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement physical GCP BigQuery provisioning (Dataset creation on Asset registration, Table creation with discovery schema on Pipeline registration) and real batch data loading in Airflow DAG execution tasks.

**Architecture:**
- `DwhProvisionerAdapter` interface in `app/application/shared/adapters/dwh_provisioner_adapter.py` decoupled from cloud engines via Clean Architecture.
- `BigQueryProvisioner` infrastructure implementation mapping domain entities and discovery metadata (`SchemaSnapshot`) to BigQuery datasets and tables.
- `RegisterAssetUseCase` & `RegisterPipelineUseCase` calling `DwhProvisionerAdapter` for physical setup.
- `BigQueryDwhLoader` executing batch load jobs via `google.cloud.bigquery.Client.load_table_from_uri`.

**Tech Stack:** Python 3.11, `google-cloud-bigquery`, `pytest`, `unittest.mock`.

## Global Constraints

- Clean Architecture: Domain/Application must not depend on `google-cloud-bigquery` directly.
- TDD: Every feature/step starts with a failing test.
- Git Protocol (M010): Do NOT run git state-modifying commands directly. Prepare ready-to-run git commands for user execution.

---

### Task 1: DWH Provisioner Port & NoOp Adapter

**Files:**
- Create: `app/application/shared/adapters/dwh_provisioner_adapter.py`
- Create: `app/infrastructure/dwh_provisioners/noop_provisioner.py`
- Test: `tests/unit/infrastructure/dwh_provisioners/test_noop_provisioner.py`

**Interfaces:**
- Consumes: None
- Produces: `DwhProvisionerAdapter` protocol (`ensure_dataset_exists`, `ensure_table_exists`) and `NoOpDwhProvisioner`.

- [ ] **Step 1: Write failing test for NoOpDwhProvisioner**

```python
# tests/unit/infrastructure/dwh_provisioners/test_noop_provisioner.py
from app.infrastructure.dwh_provisioners.noop_provisioner import NoOpDwhProvisioner
from app.application.shared.adapters.dwh_provisioner_adapter import DwhProvisionerAdapter

def test_noop_provisioner_implements_protocol():
    provisioner = NoOpDwhProvisioner()
    assert isinstance(provisioner, DwhProvisionerAdapter)

async def test_noop_provisioner_methods_executes_without_error():
    provisioner = NoOpDwhProvisioner()
    await provisioner.ensure_dataset_exists("demo_dataset", description="desc", labels={"env": "demo"})
    await provisioner.ensure_table_exists("demo_dataset", "demo_table", description="desc", labels={}, schema_fields=[])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/infrastructure/dwh_provisioners/test_noop_provisioner.py -v`
Expected: FAIL with ImportError (modules do not exist)

- [ ] **Step 3: Write minimal implementation**

```python
# app/application/shared/adapters/dwh_provisioner_adapter.py
from __future__ import annotations
from typing import Any, Protocol, runtime_checkable

@runtime_checkable
class DwhProvisionerAdapter(Protocol):
    async def ensure_dataset_exists(
        self, dataset_id: str, description: str = "", labels: dict[str, str] | None = None
    ) -> None:
        ...

    async def ensure_table_exists(
        self,
        dataset_id: str,
        table_id: str,
        description: str = "",
        labels: dict[str, str] | None = None,
        schema_fields: list[dict[str, Any]] | None = None,
    ) -> None:
        ...
```

```python
# app/infrastructure/dwh_provisioners/noop_provisioner.py
from __future__ import annotations
from typing import Any

class NoOpDwhProvisioner:
    async def ensure_dataset_exists(
        self, dataset_id: str, description: str = "", labels: dict[str, str] | None = None
    ) -> None:
        pass

    async def ensure_table_exists(
        self,
        dataset_id: str,
        table_id: str,
        description: str = "",
        labels: dict[str, str] | None = None,
        schema_fields: list[dict[str, Any]] | None = None,
    ) -> None:
        pass
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/infrastructure/dwh_provisioners/test_noop_provisioner.py -v`
Expected: PASS

---

### Task 2: Integrate DwhProvisioner into RegisterAssetUseCase & RegisterPipelineUseCase

**Files:**
- Modify: `app/application/assets/register_asset.py`
- Modify: `app/application/pipelines/register_pipeline.py`
- Test: `tests/unit/application/assets/test_register_asset.py`
- Test: `tests/unit/application/pipelines/test_register_pipeline.py`

**Interfaces:**
- Consumes: `DwhProvisionerAdapter`
- Produces: DataAsset and Pipeline registration invoking physical DWH provisioning.

- [ ] **Step 1: Write failing test in Asset Registration for DWH Provisioning**

```python
# snippet for tests/unit/application/assets/test_register_asset.py
from unittest.mock import AsyncMock
from app.application.assets.register_asset import RegisterAssetUseCase
from app.infrastructure.dwh_provisioners.noop_provisioner import NoOpDwhProvisioner

async def test_register_asset_calls_dwh_provisioner(uow, catalog, notifications):
    mock_provisioner = AsyncMock(spec=NoOpDwhProvisioner)
    use_case = RegisterAssetUseCase(uow=uow, catalog=catalog, notifications=notifications, dwh_provisioner=mock_provisioner)
    asset = await use_case.execute(
        name="sales_domain",
        description="Sales data domain",
        owner_email="owner@company.com",
        tags=["demo"],
        policy_tags=[],
        discovery_schedule="0 * * * *",
        discovery_scope_include=["*"],
        discovery_scope_exclude=[],
    )
    mock_provisioner.ensure_dataset_exists.assert_awaited_once()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/application/assets/test_register_asset.py -v`
Expected: FAIL (argument `dwh_provisioner` unexpected or method not called)

- [ ] **Step 3: Implement DWH Provisioner call in RegisterAssetUseCase & RegisterPipelineUseCase**

In `RegisterAssetUseCase.__init__`:
```python
def __init__(
    self,
    uow: UnitOfWork,
    catalog: CatalogAdapter,
    notifications: NotificationPort,
    dwh_provisioner: DwhProvisionerAdapter | None = None,
) -> None:
    self._uow = uow
    self._catalog = catalog
    self._notifications = notifications
    self._dwh_provisioner = dwh_provisioner or NoOpDwhProvisioner()
```

In `RegisterAssetUseCase.execute`:
```python
labels = {"managed_by": "clean_data_platform", "owner": asset.owner.value.replace("@", "_at_")}
for tag in asset.tags:
    labels[tag] = "true"

await self._dwh_provisioner.ensure_dataset_exists(
    dataset_id=asset.name,
    description=asset.description,
    labels=labels,
)
```

In `RegisterPipelineUseCase.__init__` and `execute`:
Inject `dwh_provisioner` and call `ensure_table_exists` for destination objects.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/application/assets/test_register_asset.py tests/unit/application/pipelines/test_register_pipeline.py -v`
Expected: PASS

---

### Task 3: BigQuery DWH Provisioner Implementation

**Files:**
- Create: `app/infrastructure/dwh_provisioners/bigquery_provisioner.py`
- Test: `tests/unit/infrastructure/dwh_provisioners/test_bigquery_provisioner.py`

**Interfaces:**
- Consumes: `google.cloud.bigquery.Client`
- Produces: `BigQueryProvisioner` implementing `DwhProvisionerAdapter`.

- [ ] **Step 1: Write failing test for BigQueryProvisioner using Mocks**

```python
# tests/unit/infrastructure/dwh_provisioners/test_bigquery_provisioner.py
from unittest.mock import MagicMock, patch
import pytest
from app.infrastructure.dwh_provisioners.bigquery_provisioner import BigQueryProvisioner

@pytest.mark.asyncio
async def test_bigquery_provisioner_creates_dataset():
    mock_client = MagicMock()
    provisioner = BigQueryProvisioner(client=mock_client)
    await provisioner.ensure_dataset_exists("raw_customers", description="Raw dataset", labels={"env": "prod"})
    mock_client.create_dataset.assert_called_once()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/infrastructure/dwh_provisioners/test_bigquery_provisioner.py -v`
Expected: FAIL with ImportError

- [ ] **Step 3: Implement BigQueryProvisioner**

```python
# app/infrastructure/dwh_provisioners/bigquery_provisioner.py
from __future__ import annotations
from typing import Any
from app.application.shared.adapters.dwh_provisioner_adapter import DwhProvisionerAdapter

class BigQueryProvisioner(DwhProvisionerAdapter):
    def __init__(self, client: Any = None) -> None:
        self._client = client

    def _get_client(self) -> Any:
        if self._client is None:
            from google.cloud import bigquery
            self._client = bigquery.Client()
        return self._client

    async def ensure_dataset_exists(
        self, dataset_id: str, description: str = "", labels: dict[str, str] | None = None
    ) -> None:
        from google.cloud import bigquery
        client = self._get_client()
        dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")
        dataset.description = description
        if labels:
            dataset.labels = labels
        client.create_dataset(dataset, exists_ok=True)

    async def ensure_table_exists(
        self,
        dataset_id: str,
        table_id: str,
        description: str = "",
        labels: dict[str, str] | None = None,
        schema_fields: list[dict[str, Any]] | None = None,
    ) -> None:
        from google.cloud import bigquery
        client = self._get_client()
        table_ref = f"{client.project}.{dataset_id}.{table_id}"

        bq_schema = []
        if schema_fields:
            for field in schema_fields:
                bq_schema.append(
                    bigquery.SchemaField(
                        name=field["name"],
                        field_type=field.get("type", "STRING").upper(),
                        mode=field.get("mode", "NULLABLE"),
                    )
                )

        table = bigquery.Table(table_ref, schema=bq_schema)
        table.description = description
        if labels:
            table.labels = labels
        client.create_table(table, exists_ok=True)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/infrastructure/dwh_provisioners/test_bigquery_provisioner.py -v`
Expected: PASS

---

### Task 4: BigQuery DWH Loader Adapter Implementation

**Files:**
- Modify: `app/infrastructure/dwh_loaders/bigquery_loader.py`
- Test: `tests/unit/infrastructure/dwh_loaders/test_bigquery_loader.py`

**Interfaces:**
- Consumes: `google.cloud.bigquery.Client`
- Produces: Complete batch load in `BigQueryDwhLoader.load()`.

- [ ] **Step 1: Write failing test for BigQueryDwhLoader.load()**

```python
# tests/unit/infrastructure/dwh_loaders/test_bigquery_loader.py
from unittest.mock import MagicMock, patch
from app.infrastructure.dwh_loaders.bigquery_loader import BigQueryDwhLoader

def test_bigquery_dwh_loader_executes_load_table_from_uri():
    mock_client = MagicMock()
    mock_job = MagicMock()
    mock_job.output_rows = 150
    mock_client.load_table_from_uri.return_value = mock_job

    loader = BigQueryDwhLoader(client=mock_client)
    res = loader.load(
        staging_path="gs://my-bucket/staging/customers.parquet",
        schema_path="",
        file_format="parquet",
        connection_metadata={"dataset": "raw", "table": "customers"},
    )
    assert res.rows_loaded == 150
    assert res.engine == "bigquery"
    mock_job.result.assert_called_once()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/infrastructure/dwh_loaders/test_bigquery_loader.py -v`
Expected: FAIL (returns `rows_loaded=0` stub)

- [ ] **Step 3: Complete BigQueryDwhLoader implementation**

```python
# app/infrastructure/dwh_loaders/bigquery_loader.py
from __future__ import annotations
from typing import Any
from app.infrastructure.airflow_callbacks.dwh_loader_adapter import DwhLoadResult

class BigQueryDwhLoader:
    def __init__(self, client: Any = None) -> None:
        self._client = client

    def _get_client(self) -> Any:
        if self._client is None:
            from google.cloud import bigquery
            self._client = bigquery.Client()
        return self._client

    def load(
        self,
        *,
        staging_path: str,
        schema_path: str,
        file_format: str,
        connection_metadata: dict[str, Any],
        resolved_credentials: dict[str, Any] | None = None,
    ) -> DwhLoadResult:
        from google.cloud import bigquery

        client = self._get_client()
        dataset = connection_metadata.get("dataset", "default")
        table = connection_metadata.get("table", "staging_table")
        table_ref = f"{client.project}.{dataset}.{table}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET if file_format.lower() == "parquet" else bigquery.SourceFormat.AVRO,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        )

        load_job = client.load_table_from_uri(
            staging_path,
            table_ref,
            job_config=job_config,
        )
        load_job.result()  # Waits for the job to complete.

        return DwhLoadResult(rows_loaded=getattr(load_job, "output_rows", 0), engine="bigquery")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/infrastructure/dwh_loaders/test_bigquery_loader.py -v`
Expected: PASS
