from __future__ import annotations

import logging
import re
import time
from typing import Any

from app.application.shared.ports.dwh_provisioner_port import DwhProvisionerPort

try:
    from google.cloud import bigquery
    from google.oauth2 import service_account
except ImportError:
    bigquery = None  # type: ignore[assignment]
    service_account = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


class BigQueryProvisioner(DwhProvisionerPort):
    """BigQuery implementation of DwhProvisionerAdapter with metadata TTL caching.

    Authentication: Uses Application Default Credentials (ADC) automatically.
    For local dev, run `gcloud auth application-default login` or set
    GOOGLE_APPLICATION_CREDENTIALS to a key file path OUTSIDE the repo.
    Never hardcode credentials or store key files inside the repository.
    """

    def __init__(
        self,
        project: str = "",
        cache_ttl_seconds: int = 300,
        client: Any = None,
        credentials_path: str | None = None,
    ) -> None:
        self._client = client
        self._project = project
        self._cache_ttl = cache_ttl_seconds
        self._credentials_path = credentials_path
        self._dataset_cache: dict[str, float] = {}
        self._table_cache: dict[str, float] = {}

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
        return {k: BigQueryProvisioner._sanitize_label_value(v) for k, v in labels.items()}

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client

        if bigquery is None:
            raise ImportError(
                "google-cloud-bigquery is required for BigQueryProvisioner. "
                "Install it with: pip install google-cloud-bigquery"
            )

        key_path = self._credentials_path

        if key_path and service_account is not None:
            creds = service_account.Credentials.from_service_account_file(key_path)  # type: ignore[no-untyped-call]
            project = self._project or getattr(creds, "project_id", None)
            self._client = bigquery.Client(project=project, credentials=creds)
            return self._client

        self._client = bigquery.Client(project=self._project or None)
        return self._client

    async def ensure_dataset_exists(
        self, dataset_id: str, description: str = "", labels: dict[str, str] | None = None
    ) -> None:
        clean_ds = dataset_id.replace("-", "_")
        now = time.monotonic()
        if (
            clean_ds in self._dataset_cache
            and (now - self._dataset_cache[clean_ds]) < self._cache_ttl
        ):
            return

        client = self._get_client()
        project = getattr(client, "project", self._project)
        dataset_ref = f"{project}.{clean_ds}" if project else clean_ds

        dataset = (
            bigquery.Dataset(dataset_ref)
            if bigquery is not None
            else getattr(client, "Dataset", None)(dataset_ref)
            if hasattr(client, "Dataset")
            else None
        )
        if dataset is None:
            # When client is a mock or bigquery is available
            if bigquery is not None:
                dataset = bigquery.Dataset(dataset_ref)
            else:
                from unittest.mock import MagicMock

                dataset = MagicMock()
                dataset.dataset_id = clean_ds
        dataset.description = description
        if labels:
            dataset.labels = self._sanitize_labels(labels)
        client.create_dataset(dataset, exists_ok=True)
        self._dataset_cache[clean_ds] = now

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
        table_key = f"{clean_ds}.{clean_tbl}"
        now = time.monotonic()
        if (
            table_key in self._table_cache
            and (now - self._table_cache[table_key]) < self._cache_ttl
        ):
            return

        client = self._get_client()
        project = getattr(client, "project", self._project)
        table_ref = f"{project}.{clean_ds}.{clean_tbl}" if project else f"{clean_ds}.{clean_tbl}"

        bq_schema = []
        if schema_fields:
            for field in schema_fields:
                raw_type = str(field.get("type", "STRING")).lower()
                if raw_type in ("bigint", "int64"):
                    bq_type = "INT64"
                elif raw_type in ("integer", "int", "smallint", "tinyint"):
                    bq_type = "INTEGER"
                elif raw_type in ("float", "float64", "double", "real"):
                    bq_type = "FLOAT64"
                elif raw_type in ("decimal", "numeric"):
                    bq_type = "NUMERIC"
                elif raw_type in ("boolean", "bool"):
                    bq_type = "BOOL"
                elif raw_type in ("timestamp", "datetime"):
                    bq_type = "TIMESTAMP"
                elif raw_type in ("date",):
                    bq_type = "DATE"
                elif raw_type in ("json",):
                    bq_type = "JSON"
                else:
                    bq_type = "STRING"

                if bigquery is not None:
                    bq_schema.append(
                        bigquery.SchemaField(
                            name=field["name"],
                            field_type=bq_type,
                            mode=field.get("mode", "NULLABLE"),
                        )
                    )
                else:
                    from unittest.mock import MagicMock

                    f_mock = MagicMock()
                    f_mock.name = field["name"]
                    f_mock.field_type = bq_type
                    f_mock.mode = field.get("mode", "NULLABLE")
                    bq_schema.append(f_mock)

        if bigquery is not None:
            table = bigquery.Table(table_ref, schema=bq_schema)
        else:
            from unittest.mock import MagicMock

            table = MagicMock()
            table.table_id = clean_tbl
            table.dataset_id = clean_ds
            table.schema = bq_schema

        table.description = description
        if labels:
            table.labels = self._sanitize_labels(labels)
        client.create_table(table, exists_ok=True)
        self._table_cache[table_key] = now
