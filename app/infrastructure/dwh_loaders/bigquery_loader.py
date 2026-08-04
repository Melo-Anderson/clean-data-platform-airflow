from __future__ import annotations

import os
from typing import Any

from app.infrastructure.airflow_callbacks.dwh_loader_adapter import DwhLoadResult


class BigQueryDwhLoader:
    """Adapter for batch loading via Google BigQuery.

    Authentication: Uses Application Default Credentials (ADC) automatically.
    For local dev, run `gcloud auth application-default login` or set
    GOOGLE_APPLICATION_CREDENTIALS=/path/outside/repo/key.json in .env.
    Never hardcode credentials or store key files inside the repository.
    """

    def __init__(self, client: Any = None, project: str | None = None) -> None:
        self._client = client
        if project:
            self._project = project
        else:
            from app.config import get_settings

            self._project = os.environ.get("PLATFORM_GCP_PROJECT", "") or get_settings().gcp_project

    def _get_bq_module(self) -> Any:
        try:
            from google.cloud import bigquery

            return bigquery
        except ImportError:

            class DummySourceFormat:
                PARQUET = "PARQUET"
                AVRO = "AVRO"

            class DummyWriteDisposition:
                WRITE_APPEND = "WRITE_APPEND"

            class DummyCreateDisposition:
                CREATE_IF_NEEDED = "CREATE_IF_NEEDED"

            class DummyLoadJobConfig:
                def __init__(
                    self,
                    source_format: Any = None,
                    write_disposition: Any = None,
                    create_disposition: Any = None,
                ) -> None:
                    self.source_format = source_format
                    self.write_disposition = write_disposition
                    self.create_disposition = create_disposition

            class DummyJob:
                def __init__(self, output_rows: int = 0) -> None:
                    self.output_rows = output_rows

                def result(self) -> None:
                    pass

            class DummyClient:
                def __init__(self, project: str | None = None) -> None:
                    self.project = project or "dummy-project"

                def load_table_from_uri(
                    self, source_uri: str, destination: Any, job_config: Any = None
                ) -> Any:
                    return DummyJob(output_rows=0)

                def load_table_from_file(
                    self, file_obj: Any, destination: Any, job_config: Any = None
                ) -> Any:
                    return DummyJob(output_rows=0)

            class DummyBQ:
                SourceFormat = DummySourceFormat
                WriteDisposition = DummyWriteDisposition
                CreateDisposition = DummyCreateDisposition
                LoadJobConfig = DummyLoadJobConfig
                Client = DummyClient

            return DummyBQ

    def _get_client(self) -> Any:
        if self._client is None:
            bq = self._get_bq_module()
            key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
            if key_path and os.path.exists(key_path):
                from google.oauth2 import service_account

                creds = service_account.Credentials.from_service_account_file(key_path)  # type: ignore[no-untyped-call]
                project = self._project or getattr(creds, "project_id", None)
                self._client = bq.Client(project=project, credentials=creds)
            else:
                self._client = bq.Client(project=self._project or None)
        return self._client

    def load(
        self,
        *,
        staging_path: str,
        schema_path: str,
        file_format: str,
        connection_metadata: dict[str, Any],
        resolved_credentials: dict[str, Any] | None = None,
    ) -> DwhLoadResult:
        bq = self._get_bq_module()
        client = self._get_client()

        dataset_name = connection_metadata.get("dataset")
        table_name = connection_metadata.get("table")
        project = getattr(client, "project", self._project)

        if hasattr(bq, "Dataset") and hasattr(client, "create_dataset"):
            try:
                ds_ref = f"{project}.{dataset_name}" if project else dataset_name
                ds_obj = bq.Dataset(ds_ref)
                client.create_dataset(ds_obj, exists_ok=True)
            except Exception:
                pass

        table_ref = (
            f"{project}.{dataset_name}.{table_name}" if project else f"{dataset_name}.{table_name}"
        )

        job_config = bq.LoadJobConfig(
            source_format=(
                bq.SourceFormat.PARQUET
                if file_format.lower() == "parquet"
                else bq.SourceFormat.AVRO
            ),
            write_disposition=bq.WriteDisposition.WRITE_APPEND,
            create_disposition=bq.CreateDisposition.CREATE_IF_NEEDED,
            autodetect=True,
            schema_update_options=[
                bq.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            ],
        )

        if staging_path.startswith("gs://"):
            load_job = client.load_table_from_uri(
                staging_path,
                table_ref,
                job_config=job_config,
            )
        elif os.path.exists(staging_path):
            with open(staging_path, "rb") as source_file:
                load_job = client.load_table_from_file(
                    source_file,
                    table_ref,
                    job_config=job_config,
                )
        else:
            load_job = client.load_table_from_uri(
                staging_path,
                table_ref,
                job_config=job_config,
            )
        load_job.result()

        rows = getattr(load_job, "output_rows", 0) or 0
        import logging

        logging.getLogger(__name__).info(
            "BigQuery load complete: target=%s, rows_loaded=%d, job_id=%s",
            table_ref,
            rows,
            getattr(load_job, "job_id", "unknown"),
        )
        return DwhLoadResult(rows_loaded=rows, engine="bigquery")
