from __future__ import annotations

import pytest


def test_classify_schema_changes_passes_on_compatible_schemas() -> None:
    """Schemas compatíveis não levantam exceção e retornam can_proceed=True."""
    from app.infrastructure.airflow_callbacks.etl_callbacks import classify_schema_changes

    source_models = {
        "prev": {"object_id": "orders", "fields": [{"name": "id", "type": "integer"}]},
        "curr": {
            "object_id": "orders",
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "name", "type": "string"},
            ],
        },
    }
    result = classify_schema_changes(source_models=source_models)
    assert result["can_proceed"] is True


def test_classify_schema_changes_raises_on_incompatible_drift() -> None:
    """Drift incompatível levanta PlatformValidationError com campo identificado."""
    from app.domain.shared.exceptions import PlatformValidationError
    from app.infrastructure.airflow_callbacks.etl_callbacks import classify_schema_changes

    source_models = {
        "prev": {"object_id": "orders", "fields": [{"name": "amount", "type": "integer"}]},
        "curr": {"object_id": "orders", "fields": [{"name": "amount", "type": "string"}]},
    }
    with pytest.raises(PlatformValidationError, match="amount"):
        classify_schema_changes(source_models=source_models)


def test_submit_transformation_job_returns_job_id_and_timestamp() -> None:
    """submit_transformation_job deve retornar job_id e submitted_at ISO."""
    from app.infrastructure.airflow_callbacks.etl_callbacks import submit_transformation_job

    result = submit_transformation_job(
        pipeline_id="p-001",
        transform_engine="dbt",
        transform_ref="models/orders.sql",
        compute_config={"engine": "dbt", "num_workers": 2},
    )
    assert "job_id" in result
    assert result["job_id"].startswith("dbt-job-")
    assert "submitted_at" in result


def test_submit_transformation_job_falls_back_for_unknown_engine() -> None:
    """Engine desconhecida retorna job_id do DummyAdapter sem exceção."""
    from app.infrastructure.airflow_callbacks.etl_callbacks import submit_transformation_job

    result = submit_transformation_job(
        pipeline_id="p-001",
        transform_engine="dataform",
        transform_ref="workflows/orders",
        compute_config={},
    )
    assert "job_id" in result
    assert "submitted_at" in result
