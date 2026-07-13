# Design Specification: ETL Pipeline Rigor (dbt/Dataform & DriftClassifier)

**Date:** 2026-07-13
**Area:** Ingestion / ETL / Pipeline Execution / Schema Drift
**Status:** Approved for Implementation
**Group:** 4 (Platform Scale - Enhancements)

---

## 1. Context & Motivation

Currently, the platform supports generating Airflow 3 DAG files for `etl` and `export` pipeline types, but the runtime callbacks representing their steps (`app/infrastructure/airflow_callbacks/etl_callbacks.py` and `export_callbacks.py`) rely on simple stubs returning hardcoded true values. Additionally, the `DriftClassifier` class is a placeholder that does not evaluate actual schema changes.

To improve the platform's execution safety and runtime operational parity with production environments:
1. **Schema Drift Classification:** Convert `DriftClassifier` into a functional class that leverages our existing `SchemaDiffer` to automatically classify and reject incompatible source model schema changes (e.g., column drops or incompatible types) before triggering transformation tasks.
2. **dbt/Dataform Execution Simulation:** Implement a `DbtComputeAdapter` following the `ComputeJobAdapter` interface to simulate the asynchronous CLI execution of dbt or Dataform transformations, including status polling, latency increments, and mock execution metrics generation.
3. **Robust ETL Callback Integration:** Integrate the `DriftClassifier` and new adapter into `etl_callbacks.py` to raise validation exceptions when drift contracts are broken, mimicking real quality gates.

---

## 2. Real DriftClassifier Implementation

The `DriftClassifier` (`app/infrastructure/drift_classifier.py`) will be refactored to parse source schema definitions (snapshots) and evaluate them using `SchemaDiffer`.

### 2.1 Interface & Payload Structure
The `classify_models` method will accept a dictionary payload representing the previous (`prev`) and current (`curr`) schema models:

```python
source_models = {
    "prev": {
        "object_id": "orders_clean",
        "fields": [
            {"name": "order_id", "type": "integer"},
            {"name": "amount", "type": "float"}
        ]
    },
    "curr": {
        "object_id": "orders_clean",
        "fields": [
            {"name": "order_id", "type": "string"}, # Incompatible type change
            # "amount" dropped
        ]
    }
}
```

### 2.2 Classification Logic
1. Parse the `prev` and `curr` definitions into domain objects: `SchemaSnapshot` and `SchemaField`.
2. Execute the existing `SchemaDiffer.diff(prev, curr)` to obtain drift events.
3. Check for any events matching `column_removed` or `type_incompatible` classifications.
4. If incompatible drift is present, return `{"can_proceed": False, "blocked_reason": "..."}` outlining the offending columns and changes.
5. Otherwise, return `{"can_proceed": True, "blocked_reason": ""}` (allowing compatible drifts such as column additions or type widening).

---

## 3. DbtComputeAdapter Design

A new adapter `DbtComputeAdapter` will be created under `app/infrastructure/adapters/compute/dbt_compute_adapter.py`.

### 3.1 Contract Implementation
It implements the `ComputeJobAdapter` protocol:
- **`submit_job(pipeline_id, pipeline_type, config)`**: Generates a unique UUID-based `job_id` (e.g., `dbt-job-<uuid>`). Initializes a simple local dictionary status tracker `self._jobs[job_id] = 0`.
- **`poll_job_status(job_id)`**:
  - Increments the poll attempts counter for the `job_id`.
  - If attempts `< 2`: Returns `JobStatus.RUNNING`.
  - If attempts `>= 2`: Returns `JobStatus.SUCCESS` with mock metrics:
    ```python
    {
        "models_run": 8,
        "tests_passed": 12,
        "rows_affected": 5400,
        "metrics_path": f"/tmp/metrics_{job_id}.json"
    }
    ```
- **`cancel_job(job_id)`**: Clean up the status tracking dictionary.

### 3.2 Factory Integration
In `app/infrastructure/compute_job_factory.py`, update `get_transform_adapter` to return `DbtComputeAdapter` instead of `DummyComputeAdapter`.

---

## 4. Callback Refactoring (`etl_callbacks.py`)

Update `app/infrastructure/airflow_callbacks/etl_callbacks.py` to leverage these features:

### 4.1 Schema Verification Gate
```python
def classify_schema_changes(*, source_models: dict[str, Any]) -> dict[str, Any]:
    """Classify schema changes. Raises PlatformValidationError if incompatible."""
    result = DriftClassifier().classify_models(source_models=source_models)
    if not result["can_proceed"]:
        from app.domain.shared.exceptions import PlatformValidationError
        raise PlatformValidationError(result["blocked_reason"])
    return result
```

### 4.2 Submit Transformation
```python
def submit_transformation_job(
    *,
    pipeline_id: str,
    transform_engine: str,
    transform_ref: str,
    compute_config: dict[str, Any],
) -> dict[str, str]:
    """Submit dbt or Dataform transformation job via transform adapter."""
    adapter = get_transform_adapter(transform_engine)
    job_id = adapter.submit_job(
        pipeline_id=pipeline_id,
        pipeline_type="etl",
        config={"ref": transform_ref, **compute_config},
    )
    return {"job_id": job_id, "submitted_at": datetime.now(tz=UTC).isoformat()}
```

---

## 5. Verification Plan

### 5.1 Automated Tests
1. **DriftClassifier Tests (`tests/unit/infrastructure/test_drift_classifier.py`):**
   - Assert compatible schema changes (column added, type widened) return `can_proceed: True`.
   - Assert incompatible changes (column removed, type changed incompatibly) return `can_proceed: False` with descriptive `blocked_reason`.
2. **DbtComputeAdapter Tests (`tests/unit/infrastructure/adapters/test_dbt_adapter.py`):**
   - Verify job status transitions: `RUNNING` on first poll -> `SUCCESS` on second poll.
   - Verify job metrics are populated on success.
3. **ETL Callback Integration Tests (`tests/unit/infrastructure/test_etl_callbacks.py`):**
   - Verify `classify_schema_changes` successfully runs and passes on valid schemas.
   - Verify `classify_schema_changes` raises `PlatformValidationError` on incompatible schemas.
