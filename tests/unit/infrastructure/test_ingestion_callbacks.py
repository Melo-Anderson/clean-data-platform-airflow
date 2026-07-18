from __future__ import annotations

from unittest.mock import patch

import pytest

from app.infrastructure.airflow_callbacks.dwh_loader_adapter import DwhLoadResult
from app.infrastructure.airflow_callbacks.ingestion_callbacks import (
    load_to_data_warehouse,
    post_load_validation,
)


class FakeDwhLoader:
    """Named loader for testing — does not use anonymous MagicMock (clean-code.md §3)."""

    def load(
        self,
        *,
        staging_path,
        schema_path,
        file_format,
        connection_metadata,
        resolved_credentials=None,
    ):
        return DwhLoadResult(rows_loaded=1000, engine="bigquery")


def test_load_to_data_warehouse_delegates_to_loader() -> None:
    with patch(
        "app.infrastructure.dwh_loaders.dwh_loader_factory.get_dwh_loader",
        return_value=FakeDwhLoader(),
    ):
        result = load_to_data_warehouse(
            pipeline_id="p1",
            destination_object_ids=["obj-1"],
            staging_path="/tmp/out.parquet",
            schema_path="/tmp/schema.json",
            engine_type="bigquery",
            file_format="parquet",
            connection_metadata={"dataset": "ds", "table": "tbl"},
            auth_method="iam",
        )
    assert result["loaded"] is True
    assert result["rows_loaded"] == 1000


def test_load_to_data_warehouse_resolves_vault_when_auth_method_vault() -> None:
    class FakeClient:
        def resolve_vault_secrets(self, ref: str) -> dict:
            return {"token": "secret-token"}

    with (
        patch(
            "app.infrastructure.dwh_loaders.dwh_loader_factory.get_dwh_loader",
            return_value=FakeDwhLoader(),
        ),
        patch(
            "app.infrastructure.airflow_callbacks.ingestion_callbacks.get_platform_client",
            return_value=FakeClient(),
        ),
    ):
        result = load_to_data_warehouse(
            pipeline_id="p1",
            destination_object_ids=["obj-1"],
            staging_path="/tmp/out.parquet",
            schema_path="/tmp/schema.json",
            engine_type="bigquery",
            file_format="parquet",
            connection_metadata={},
            auth_method="vault",
            credential_ref="secret/bigquery",
        )
    assert result["loaded"] is True


def test_post_load_validation_passes_when_delta_is_zero() -> None:
    result = post_load_validation(
        pipeline_id="p1",
        expected_rows=1000,
        actual_rows=1000,
        source_checksum=None,
        destination_checksum=None,
    )
    assert result["validation_ok"] is True
    assert result["row_delta_pct"] == 0.0


def test_post_load_validation_raises_when_delta_exceeds_threshold() -> None:
    with pytest.raises(RuntimeError, match="delta exceeds 0.5% threshold"):
        post_load_validation(
            pipeline_id="p1",
            expected_rows=1000,
            actual_rows=1010,  # 1.0% delta — above 0.5%
            source_checksum=None,
            destination_checksum=None,
        )


def test_post_load_validation_passes_within_threshold() -> None:
    # 3 rows off in 1000 = 0.3% — below the 0.5% threshold
    result = post_load_validation(
        pipeline_id="p1",
        expected_rows=1000,
        actual_rows=1003,
        source_checksum=None,
        destination_checksum=None,
    )
    assert result["validation_ok"] is True


def test_post_load_validation_raises_on_checksum_mismatch() -> None:
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        post_load_validation(
            pipeline_id="p1",
            expected_rows=1000,
            actual_rows=1000,
            source_checksum="abc123",
            destination_checksum="def456",
        )
