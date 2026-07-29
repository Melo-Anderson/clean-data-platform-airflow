"""Tests for PipelineSpec — the canonical YAML contract."""

import pytest

from src.domain.schemas.pipeline_spec import (
    AirflowSpec,
    ComputeSpec,
    DestinationObjectSpec,
    DestinationSpec,
    DiscoveryTaskSpec,
    ExtractionSpec,
    PipelineSpec,
    QualityRuleSpec,
    QualitySpec,
    ScheduleSpec,
    SensorSpec,
    SourceSpec,
    TransformSpec,
)


def test_schedule_cron_requires_cron_expression() -> None:
    with pytest.raises(ValueError, match="cron_schedule"):
        ScheduleSpec(mode="cron")


def test_schedule_cron_valid() -> None:
    s = ScheduleSpec(mode="cron", cron="0 6 * * *")
    assert s.cron == "0 6 * * *"


def test_schedule_cron_rejects_invalid_expression() -> None:
    with pytest.raises(ValueError, match="Invalid cron"):
        ScheduleSpec(mode="cron", cron="99 99 * *")


def test_schedule_trigger_requires_depends_on() -> None:
    with pytest.raises(ValueError, match="depends_on"):
        ScheduleSpec(mode="trigger")


def test_schedule_trigger_valid() -> None:
    s = ScheduleSpec(
        mode="trigger",
        depends_on=[
            {"pipeline_id": "pipe_a", "require_same_day": True, "dependency_type": "dataset"}
        ],
    )
    assert len(s.depends_on) == 1


def test_schedule_trigger_with_gate_requires_both() -> None:
    with pytest.raises(ValueError):
        ScheduleSpec(mode="trigger_with_gate", cron="0 6 * * *")  # missing depends_on


def test_schedule_trigger_forbids_cron() -> None:
    with pytest.raises(ValueError, match="cron"):
        ScheduleSpec(
            mode="trigger",
            cron="0 6 * * *",
            depends_on=[
                {"pipeline_id": "p", "require_same_day": False, "dependency_type": "dataset"}
            ],
        )


def test_sensor_requires_non_empty_query() -> None:
    with pytest.raises(ValueError, match="query"):
        SensorSpec(query="", timeout_minutes=60, poke_interval_seconds=60)


def test_sensor_requires_positive_timeout() -> None:
    with pytest.raises(ValueError, match="timeout_minutes"):
        SensorSpec(query="SELECT 1", timeout_minutes=0, poke_interval_seconds=60)


def test_transform_dbt_requires_ref() -> None:
    with pytest.raises(ValueError, match="ref"):
        TransformSpec(engine="dbt")


def test_transform_none_no_ref_required() -> None:
    t = TransformSpec(engine="none")
    assert t.ref is None


def test_transform_dataform_requires_ref() -> None:
    with pytest.raises(ValueError, match="ref"):
        TransformSpec(engine="dataform")


def _make_valid_spec(owner: str = "eng@company.com") -> PipelineSpec:
    return PipelineSpec(
        schema_version="1.0",
        pipeline_id="p_ingestion_sales",
        name="Ingest Sales",
        type="ingestion",
        owner=owner,
        schedule=ScheduleSpec(mode="cron", cron="0 6 * * *"),
        source=SourceSpec(
            asset_id="src_postgres",
            objects=[
                ExtractionSpec(
                    object_id="sales_table",
                    load_strategy="full_load",
                    page_size=1000,
                    compression="snappy",
                    encoding="utf-8",
                )
            ],
        ),
        destination=DestinationSpec(
            asset_id="dst_bigquery",
            objects=[DestinationObjectSpec(object_id="sales_table", create_if_not_exists=True)],
        ),
        transform=TransformSpec(engine="none"),
        compute=ComputeSpec(
            engine="default",
            num_workers=1,
            machine_type="n1-standard-2",
            staging_bucket="gs://bucket",
        ),
        quality=QualitySpec(metrics=[QualityRuleSpec(type="row_count_min", value=1)]),
        airflow=AirflowSpec(),
        discovery_task=DiscoveryTaskSpec(),
    )


def test_pipeline_spec_owner_must_be_email() -> None:
    with pytest.raises(ValueError, match="owner"):
        _make_valid_spec(owner="not-an-email")


def test_pipeline_spec_valid_minimal() -> None:
    spec = _make_valid_spec()
    assert spec.pipeline_id == "p_ingestion_sales"
    assert spec.type == "ingestion"
    assert spec.schedule.mode == "cron"
