from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from app.infrastructure.airflow_callbacks.dwh_loader_adapter import DwhLoadResult

try:
    from google.cloud import bigquery
    from google.oauth2 import service_account
except ImportError:
    bigquery = None  # type: ignore[assignment]
    service_account = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


def _get_staging_files(staging_path: str) -> list[str]:
    """Resolve file paths from staging target (single file or directory)."""
    p = Path(staging_path)
    if p.is_dir():
        return [f.as_posix() for f in p.glob("*.parquet*")] + [
            f.as_posix() for f in p.glob("*.avro*")
        ]
    if p.exists():
        return [p.as_posix()]
    return []


class BigQueryDwhLoader:
    """Adapter for batch loading into Google BigQuery."""

    def __init__(
        self,
        project: str = "",
        client: Any = None,
        credentials_path: str | None = None,
    ) -> None:
        self._client = client
        self._project = project
        self._credentials_path = credentials_path

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client

        if bigquery is None:
            raise ImportError("google-cloud-bigquery is required for BigQueryDwhLoader")

        key_path = self._credentials_path

        if key_path and service_account is not None:
            creds = service_account.Credentials.from_service_account_file(key_path)  # type: ignore[no-untyped-call]
            project = self._project or getattr(creds, "project_id", None)
            self._client = bigquery.Client(project=project, credentials=creds)
            return self._client

        self._client = bigquery.Client(project=self._project or None)
        return self._client

    def _create_job_config(self, bq: Any, file_format: str) -> Any:
        fmt = bq.SourceFormat.PARQUET if file_format.lower() == "parquet" else bq.SourceFormat.AVRO
        return bq.LoadJobConfig(
            source_format=fmt,
            write_disposition=bq.WriteDisposition.WRITE_APPEND,
            create_disposition=bq.CreateDisposition.CREATE_IF_NEEDED,
            autodetect=True,
            schema_update_options=[bq.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        )

    def _ensure_dataset(self, client: Any, bq: Any, dataset_name: str) -> None:
        project = getattr(client, "project", self._project)
        ds_ref = f"{project}.{dataset_name}" if project else dataset_name
        try:
            client.create_dataset(bq.Dataset(ds_ref), exists_ok=True)
        except Exception as exc:
            logger.debug("Ensure dataset skipped/exists: %s", exc)

    def _load_single_file(
        self, client: Any, file_path: str, table_ref: str, job_config: Any
    ) -> int:
        with open(file_path, "rb") as f:
            job = client.load_table_from_file(f, table_ref, job_config=job_config)
            job.result()
            return int(getattr(job, "output_rows", 0) or 0)

    def _load_uri(self, client: Any, uri: str, table_ref: str, job_config: Any) -> int:
        job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
        job.result()
        return int(getattr(job, "output_rows", 0) or 0)

    def load(
        self,
        *,
        staging_path: str,
        schema_path: str,
        file_format: str = "parquet",
        connection_metadata: dict[str, Any],
        resolved_credentials: dict[str, Any] | None = None,
    ) -> DwhLoadResult:
        if bigquery is None:
            raise ImportError("google-cloud-bigquery is required for BigQueryDwhLoader")

        client = self._get_client()
        dataset = connection_metadata.get("dataset", "")
        table = connection_metadata.get("table", "")
        project = getattr(client, "project", self._project)

        self._ensure_dataset(client, bigquery, dataset)
        table_ref = f"{project}.{dataset}.{table}" if project else f"{dataset}.{table}"
        job_config = self._create_job_config(bigquery, file_format)

        files = _get_staging_files(staging_path)
        if files:
            total_rows = sum(
                self._load_single_file(client, f, table_ref, job_config) for f in files
            )
        else:
            total_rows = self._load_uri(client, staging_path, table_ref, job_config)

        return DwhLoadResult(rows_loaded=total_rows, engine="bigquery")
