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
        self._project = project or os.environ.get("PLATFORM_GCP_PROJECT", "")

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

        dataset = connection_metadata.get("dataset", "default")
        table = connection_metadata.get("table", "staging_table")
        project = getattr(client, "project", self._project)
        table_ref = f"{project}.{dataset}.{table}" if project else f"{dataset}.{table}"

        job_config = bq.LoadJobConfig(
            source_format=(
                bq.SourceFormat.PARQUET
                if file_format.lower() == "parquet"
                else bq.SourceFormat.AVRO
            ),
            write_disposition=bq.WriteDisposition.WRITE_APPEND,
            create_disposition=bq.CreateDisposition.CREATE_IF_NEEDED,
        )

        load_job = client.load_table_from_uri(
            staging_path,
            table_ref,
            job_config=job_config,
        )
        load_job.result()

        rows = getattr(load_job, "output_rows", 0)
        return DwhLoadResult(rows_loaded=rows, engine="bigquery")
