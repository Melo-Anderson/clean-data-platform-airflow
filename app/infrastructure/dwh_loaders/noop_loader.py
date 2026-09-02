from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from app.infrastructure.airflow_callbacks.dwh_loader_adapter import DwhLoaderAdapter, DwhLoadResult


class NoOpDwhLoader(DwhLoaderAdapter):
    """No-op DWH loader for testing, local execution, and dry-runs without cloud credentials."""

    def load(
        self,
        *,
        staging_path: str,
        schema_path: str,
        file_format: str = "parquet",
        connection_metadata: dict[str, Any] | None = None,
        resolved_credentials: dict[str, Any] | None = None,
    ) -> DwhLoadResult:
        rows = 0
        if staging_path and Path(staging_path).exists():
            p_path = Path(staging_path)
            if p_path.is_file() and p_path.suffix == ".parquet":
                with contextlib.suppress(Exception):
                    rows += pq.read_metadata(p_path).num_rows
            elif p_path.is_dir():
                for p in p_path.glob("**/*.parquet"):
                    with contextlib.suppress(Exception):
                        rows += pq.read_metadata(p).num_rows
        return DwhLoadResult(rows_loaded=rows, engine="noop")
