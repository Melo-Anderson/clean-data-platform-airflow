from __future__ import annotations

from typing import Any

from app.infrastructure.airflow_callbacks.dwh_loader_adapter import DwhLoadResult


class BigQueryDwhLoader:
    """Adapter for batch loading via Google BigQuery.

    Mechanism: google.cloud.bigquery.Client.load_table_from_uri (native async job in GCP).
    In production, requires GOOGLE_APPLICATION_CREDENTIALS or Workload Identity (auth_method="iam").
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
        # TODO(implementer): Replace with google-cloud-bigquery execution in production.
        return DwhLoadResult(rows_loaded=0, engine="bigquery")
