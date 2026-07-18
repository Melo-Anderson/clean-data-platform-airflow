from __future__ import annotations

from typing import Any

from app.infrastructure.airflow_callbacks.dwh_loader_adapter import DwhLoadResult


class DatabricksDwhLoader:
    """Adapter for loading via Databricks SQL Connector.

    Mechanism: COPY INTO <table> FROM '<staging_path>' FILEFORMAT = <format>
    Idempotency: Databricks tracks loaded files to prevent duplication on retries.
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
        # TODO(implementer): replace with databricks-sql-connector SDK in production.
        # with sql.connect(server_hostname=..., http_path=...) as conn:
        #     cursor = conn.cursor()
        #     cursor.execute(f"COPY INTO {table} FROM '{staging_path}' FILEFORMAT={format.upper()}")
        return DwhLoadResult(rows_loaded=0, engine="databricks")
