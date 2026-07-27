# Platform Contract Provider Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose a dynamic JSON Schema endpoint and canonical Gold Examples in the Data Platform for the Harness Engine to consume.

**Architecture:** Create `SchemaProvider` service in `app/infrastructure/` and add FastAPI routes to serve the JSON Schema and Few-Shot Gold YAML examples.

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, PyYAML, Pytest.

## Global Constraints

- Standalone, modular implementation in `app/infrastructure/`.
- Must export valid JSON Schema compliant with Pydantic v2 `PipelineSpec`.

---

### Task 1: Schema Provider Service & Gold Examples

**Files:**
- Create: `app/infrastructure/schema_provider.py`
- Test: `tests/unit/test_schema_provider.py`

**Interfaces:**
- Consumes: `app.domain.schemas.pipeline_spec.PipelineSpec`
- Produces: `get_pipeline_json_schema() -> dict`, `get_gold_examples() -> dict[str, str]`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_schema_provider.py
from app.infrastructure.schema_provider import get_pipeline_json_schema, get_gold_examples

def test_get_pipeline_json_schema_returns_valid_dict() -> None:
    schema = get_pipeline_json_schema()
    assert isinstance(schema, dict)
    assert "properties" in schema or "$defs" in schema

def test_get_gold_examples_returns_examples() -> None:
    examples = get_gold_examples()
    assert "ingestion" in examples
    assert "etl" in examples
    assert "export" in examples
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_schema_provider.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

```python
# app/infrastructure/schema_provider.py
from typing import Any
from app.domain.schemas.pipeline_spec import PipelineSpec

_GOLD_EXAMPLES = {
    "ingestion": """schema_version: '1.0'
pipeline_id: p_ingest_sales
name: Ingest Sales Table
type: ingestion
owner: data-eng@company.com
schedule:
  mode: cron
  cron: 0 6 * * *
source:
  asset_id: postgres_sales
  objects:
    - object_id: sales_raw
      load_strategy: full_load
      page_size: 1000
      compression: snappy
      encoding: utf-8
destination:
  asset_id: s3_lake
  objects:
    - object_id: sales_raw
      create_if_not_exists: true
transform:
  engine: none
compute:
  engine: default
  num_workers: 2
  machine_type: n1-standard-4
  staging_bucket: gs://my-staging-bucket
quality:
  metrics:
    - type: row_count_min
      value: 1
airflow:
  retries: 3
  retry_delay_seconds: 300
discovery_task:
  enabled: false
""",
    "etl": """schema_version: '1.0'
pipeline_id: p_etl_orders
name: ETL Orders Transformation
type: etl
owner: analytics@company.com
schedule:
  mode: cron
  cron: 0 8 * * *
source:
  asset_id: raw_orders
  objects:
    - object_id: orders
      load_strategy: incremental
      watermark_column: updated_at
destination:
  asset_id: dw_orders
  objects:
    - object_id: dim_orders
      create_if_not_exists: true
transform:
  engine: dbt
  ref: models/marts/dim_orders.sql
compute:
  engine: default
  num_workers: 4
  machine_type: n1-standard-8
  staging_bucket: gs://my-staging-bucket
quality:
  metrics:
    - type: row_count_min
      value: 100
airflow:
  retries: 2
  retry_delay_seconds: 180
discovery_task:
  enabled: true
""",
    "export": """schema_version: '1.0'
pipeline_id: p_export_finance
name: Export Finance Summary
type: export
owner: finance@company.com
schedule:
  mode: cron
  cron: 0 12 * * *
source:
  asset_id: dw_finance
  objects:
    - object_id: daily_summary
      load_strategy: full_load
destination:
  asset_id: external_s3
  objects:
    - object_id: summary_csv
      create_if_not_exists: true
transform:
  engine: none
compute:
  engine: default
  num_workers: 2
  machine_type: n1-standard-4
  staging_bucket: gs://my-staging-bucket
quality:
  metrics:
    - type: row_count_min
      value: 1
airflow:
  retries: 1
  retry_delay_seconds: 60
discovery_task:
  enabled: false
"""
}

def get_pipeline_json_schema() -> dict[str, Any]:
    return PipelineSpec.model_json_schema()

def get_gold_examples() -> dict[str, str]:
    return _GOLD_EXAMPLES
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_schema_provider.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/infrastructure/schema_provider.py tests/unit/test_schema_provider.py
git commit -m "feat(platform): add schema provider service and gold examples"
```
