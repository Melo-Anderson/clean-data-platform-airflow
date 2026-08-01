from __future__ import annotations

import os
import re
from typing import Any

from app.application.shared.adapters.dwh_provisioner_adapter import DwhProvisionerAdapter


class BigQueryProvisioner(DwhProvisionerAdapter):
    """BigQuery implementation of DwhProvisionerAdapter.

    Authentication: Uses Application Default Credentials (ADC) automatically.
    For local dev, run `gcloud auth application-default login` or set
    GOOGLE_APPLICATION_CREDENTIALS to a key file path OUTSIDE the repo.
    Never hardcode credentials or store key files inside the repository.
    """

    def __init__(self, client: Any = None, project: str | None = None) -> None:
        self._client = client
        self._project = project or os.environ.get("PLATFORM_GCP_PROJECT", "")

    @staticmethod
    def _sanitize_label_value(value: str) -> str:
        """Sanitize a single label value for BigQuery.

        BigQuery labels: lowercase letters, digits, underscores, hyphens, max 63 chars.
        Replace '@' with '_at_', then replace any remaining invalid char with '_'.
        """
        value = value.replace("@", "_at_")
        value = re.sub(r"[^a-z0-9_\-]", "_", value.lower())
        return value[:63]

    @staticmethod
    def _sanitize_labels(labels: dict[str, str]) -> dict[str, str]:
        return {
            k: BigQueryProvisioner._sanitize_label_value(v)
            for k, v in labels.items()
        }

    def _get_bq_module(self) -> Any:
        try:
            from google.cloud import bigquery

            return bigquery
        except ImportError:
            import logging

            logging.getLogger(__name__).warning(
                "google-cloud-bigquery is not installed. BigQueryProvisioner is running in Dummy/Mock mode."
            )
            # Fallback when google-cloud-bigquery is not installed in local/CI environment
            class DummyDataset:
                def __init__(self, dataset_id: str) -> None:
                    parts = dataset_id.split(".")
                    self.dataset_id = parts[-1]
                    self.description = ""
                    self.labels: dict[str, str] = {}

            class DummySchemaField:
                def __init__(self, name: str, field_type: str, mode: str = "NULLABLE") -> None:
                    self.name = name
                    self.field_type = field_type
                    self.mode = mode

            class DummyTable:
                def __init__(self, table_ref: str, schema: list[Any] | None = None) -> None:
                    parts = table_ref.split(".")
                    self.table_id = parts[-1]
                    self.dataset_id = parts[-2] if len(parts) > 1 else ""
                    self.description = ""
                    self.labels: dict[str, str] = {}
                    self.schema = schema or []

            class DummyClient:
                def __init__(self, project: str | None = None) -> None:
                    self.project = project or "dummy-project"

                def create_dataset(self, dataset: Any, exists_ok: bool = True) -> Any:
                    return dataset

                def create_table(self, table: Any, exists_ok: bool = True) -> Any:
                    return table

            class DummyBQ:
                Dataset = DummyDataset
                Table = DummyTable
                SchemaField = DummySchemaField
                Client = DummyClient

            return DummyBQ

    def _get_client(self) -> Any:
        if self._client is None:
            bq = self._get_bq_module()
            self._client = bq.Client(project=self._project or None)
        return self._client

    async def ensure_dataset_exists(
        self, dataset_id: str, description: str = "", labels: dict[str, str] | None = None
    ) -> None:
        clean_ds = dataset_id.replace("-", "_")
        bq = self._get_bq_module()
        client = self._get_client()
        project = getattr(client, "project", self._project)
        dataset_ref = f"{project}.{clean_ds}" if project else clean_ds

        dataset = bq.Dataset(dataset_ref)
        dataset.description = description
        if labels:
            dataset.labels = self._sanitize_labels(labels)
        client.create_dataset(dataset, exists_ok=True)

    async def ensure_table_exists(
        self,
        dataset_id: str,
        table_id: str,
        description: str = "",
        labels: dict[str, str] | None = None,
        schema_fields: list[dict[str, Any]] | None = None,
    ) -> None:
        clean_ds = dataset_id.replace("-", "_")
        clean_tbl = table_id.replace("-", "_")
        bq = self._get_bq_module()
        client = self._get_client()
        project = getattr(client, "project", self._project)
        table_ref = f"{project}.{clean_ds}.{clean_tbl}" if project else f"{clean_ds}.{clean_tbl}"

        bq_schema = []
        if schema_fields:
            for field in schema_fields:
                bq_schema.append(
                    bq.SchemaField(
                        name=field["name"],
                        field_type=field.get("type", "STRING").upper(),
                        mode=field.get("mode", "NULLABLE"),
                    )
                )

        table = bq.Table(table_ref, schema=bq_schema)
        table.description = description
        if labels:
            table.labels = self._sanitize_labels(labels)
        client.create_table(table, exists_ok=True)

