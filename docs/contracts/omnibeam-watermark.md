# OmniBeam Watermark & Offset Contract

## Overview

OmniBeam supports incremental state tracking across bounded SQL, MongoDB, and REST API sources via the `watermark` configuration block.

---

## 1. Input Manifest Contract

When a pipeline runs in incremental mode, the ingestion coordinator injects a `watermark` object into `database_source` or `api_source`:

```json
{
  "watermark": {
    "column": "updated_at",
    "type": "timestamp",
    "value": "2026-10-24T10:00:00+00:00"
  }
}
```

### Fields
- `column` (string, required): The target column or JSON field used for ordering and boundary filtering.
- `type` (string, required): Data type of the column. Supported values:
  - `integer` (64-bit integer)
  - `float` (64-bit float)
  - `timestamp` (ISO-8601 UTC timestamp)
  - `date` (YYYY-MM-DD date)
  - `string` (lexicographical string)
- `value` (string, optional / nullable): The last successfully ingested watermark value. `null` indicates initial run with no prior watermark.

---

## 2. Extraction Behavior

1. **Auto Mode (Table Extraction):**
   - When no custom SQL query is provided, OmniBeam automatically appends a filter predicate:
     `{column} > {value}` (using native typed comparison, not string comparison).
2. **Custom Query Mode:**
   - If the manifest contains a SQL query with `:watermark_value`, OmniBeam or the translator substitutes `:watermark_value` with the typed literal.
3. **REST API Mode:**
   - Query parameters containing `:watermark_value` are replaced with the raw string parameter value.

---

## 3. Output Metrics Contract (`metrics.json`)

Upon job completion, OmniBeam writes `metrics.json` containing the new watermark:

```json
{
  "rows_written": 15420,
  "bytes_written": 3145728,
  "execution_time_seconds": 12.4,
  "watermark": {
    "column": "updated_at",
    "type": "timestamp",
    "value": "2026-10-24T14:30:00+00:00"
  }
}
```

### Invariants:
1. **Value Calculation:** `watermark.value` MUST be computed as `MAX({watermark.column})` across all successfully extracted records.
2. **Zero-Row Runs (Idempotence):** If `rows_written == 0`, `watermark.value` MUST preserve the incoming `watermark.value`. It must NEVER become `null` if a previous value existed.
3. **Contract Enforcement:** If the manifest contained a `watermark` block, `metrics.json` MUST contain the `"watermark"` key with a non-empty `column` and `type`. Failure to report the watermark is treated by the platform as a fatal job failure.
