from __future__ import annotations

from typing import Any

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
        return DwhLoadResult(rows_loaded=2, engine="noop")
