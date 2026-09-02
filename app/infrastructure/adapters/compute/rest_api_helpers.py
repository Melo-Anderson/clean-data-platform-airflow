from __future__ import annotations

import base64
import contextlib
import json
from pathlib import Path
from typing import Any

import duckdb

WRAPPER_KEYS = ("data", "items", "results", "records", "content")


def build_auth_headers(auth_type: str, creds: dict[str, str]) -> dict[str, str]:
    """Build HTTP authentication headers from resolved credentials."""
    headers: dict[str, str] = {}
    normalized_type = auth_type.lower()

    if normalized_type == "bearer":
        token = creds.get("token") or creds.get("api_key") or creds.get("jwt") or ""
        if token:
            headers["Authorization"] = f"Bearer {token}"
    elif normalized_type == "api_key":
        key = creds.get("api_key") or creds.get("key") or creds.get("token") or ""
        header_name = (
            creds.get("api_key_header")
            or creds.get("header_name")
            or creds.get("header")
            or "x-api-key"
        )
        if key:
            headers[header_name] = key
    elif normalized_type == "basic":
        username = creds.get("username") or creds.get("user") or ""
        password = creds.get("password") or creds.get("pass") or ""
        if username or password:
            encoded = base64.b64encode(f"{username}:{password}".encode()).decode()
            headers["Authorization"] = f"Basic {encoded}"
    return headers


def normalize_resource_path(raw_path: str) -> str:
    """Normalize and format REST API resource path."""
    clean = raw_path.strip("/")
    if clean.startswith("api/v1/api/v1/"):
        clean = clean.replace("api/v1/api/v1/", "api/v1/")
    if clean in ("transactions", "api/v1/transactions", "v1/transactions"):
        clean = "api/v1/orders"
    elif clean and not clean.startswith("api/v1/"):
        clean = f"api/v1/{clean}"
    return f"/{clean}" if clean else "/"


def resolve_jsonpath(data: Any, path: str) -> Any:
    """Resolve dotted paths like 'pagination.next_cursor' from response dict."""
    current = data
    for part in path.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def extract_envelope_items(
    raw: Any, wrapper_keys: tuple[str, ...] = WRAPPER_KEYS
) -> list[dict[str, Any]]:
    """Extract list of records from an arbitrary JSON response or envelope."""
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    if isinstance(raw, dict):
        for key in wrapper_keys:
            val = raw.get(key)
            if isinstance(val, list):
                return [item for item in val if isinstance(item, dict)]
    return []


def calculate_parquet_metrics(parquet_path: Path) -> dict[str, Any]:
    """Calculate descriptive metrics (row counts, nulls, duplicates) for a Parquet file."""
    metrics: dict[str, Any] = {}
    if not parquet_path.exists():
        return metrics

    with duckdb.connect(database=":memory:") as conn:
        schema_rows = conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')"
        ).fetchall()
        row = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()
        row_count = row[0] if row is not None else 0
        metrics["row_count"] = row_count
        metrics["bytes_written"] = parquet_path.stat().st_size

        for col_info in schema_rows:
            col_name = col_info[0]
            null_res = conn.execute(
                f"SELECT COUNT(*) - COUNT(\"{col_name}\") FROM read_parquet('{parquet_path}')"
            ).fetchone()
            if null_res is not None:
                metrics[f"null_count_{col_name}"] = null_res[0]

            dup_res = conn.execute(
                f'SELECT COUNT("{col_name}") - COUNT(DISTINCT "{col_name}") FROM read_parquet(\'{parquet_path}\')'
            ).fetchone()
            if dup_res is not None:
                metrics[f"duplicate_count_{col_name}"] = dup_res[0]

    return metrics


def parse_extraction_query(query: str | None) -> dict[str, Any]:
    """Parse custom JSON query parameters."""
    if not query:
        return {}

    with contextlib.suppress(Exception):
        parsed = json.loads(query)
        if isinstance(parsed, dict):
            return parsed
    return {}


def build_page_params(
    pag_cfg: dict[str, Any],
    strategy: str,
    page_size: int,
    offset: int,
    page_num: int,
    cursor: str | None,
    custom_params: dict[str, Any],
) -> dict[str, Any]:
    """Build request query params for the current pagination iteration."""
    params = dict(custom_params)
    if strategy == "offset_limit":
        params[pag_cfg.get("limit_param", "limit")] = page_size
        params[pag_cfg.get("offset_param", "offset")] = offset
    elif strategy == "page_number":
        params[pag_cfg.get("limit_param", "limit")] = page_size
        params[pag_cfg.get("page_param", "page")] = page_num
    elif strategy == "cursor" and cursor is not None:
        params["cursor"] = cursor
    return params
