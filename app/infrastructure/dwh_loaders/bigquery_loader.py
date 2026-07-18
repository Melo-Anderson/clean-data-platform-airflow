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
        # TODO(implementer): replace with google-cloud-bigquery SDK in production.
        # client = bigquery.Client(credentials=resolved_credentials or None)
        # job = client.load_table_from_uri(staging_path, connection_metadata["table"])
        # job.result()  # wait for completion
        return DwhLoadResult(rows_loaded=0, engine="bigquery")
