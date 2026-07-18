from __future__ import annotations

from typing import Any

from app.infrastructure.airflow_callbacks.dwh_loader_adapter import DwhLoadResult


class SnowflakeDwhLoader:
    """Adapter for loading via Snowflake.

    Mechanism: COPY INTO <table> FROM @<stage> FILE_FORMAT = (TYPE = <format>) PURGE = FALSE
    """

    def load(
        self,
        *,
        staging_path: str,
        schema_path: str,
        file_format: str,
        connection_metadata: dict[str, Any],
        resolved_credentials: dict[str, Any] | None = None,
    ) -> DwhLoadResult:
        # TODO(implementer): Replace with snowflake-connector-python execution in production.
        return DwhLoadResult(rows_loaded=0, engine="snowflake")
