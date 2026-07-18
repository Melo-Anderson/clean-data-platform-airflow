from __future__ import annotations

from app.infrastructure.airflow_callbacks.dwh_loader_adapter import DwhLoaderAdapter, DwhLoadResult


class FakeDwhLoader:
    def load(
        self,
        *,
        staging_path: str,
        schema_path: str,
        file_format: str,
        connection_metadata: dict,
        resolved_credentials: dict | None = None,
    ) -> DwhLoadResult:
        return DwhLoadResult(rows_loaded=100, engine=file_format)


def test_dwh_loader_adapter_is_protocol_satisfied_by_fake() -> None:
    loader = FakeDwhLoader()
    assert isinstance(loader, DwhLoaderAdapter)


def test_dwh_load_result_has_required_fields() -> None:
    result = DwhLoadResult(rows_loaded=500, engine="bigquery")
    assert result.rows_loaded == 500
    assert result.engine == "bigquery"
