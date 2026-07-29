"""Tests for yaml_exporter — dump() and format_feedback_prompt()."""

import yaml

from src.domain.schemas.pipeline_spec import (
    AirflowSpec,
    ComputeSpec,
    DestinationObjectSpec,
    DestinationSpec,
    DiscoveryTaskSpec,
    ExtractionSpec,
    PipelineSpec,
    QualitySpec,
    ScheduleSpec,
    SourceSpec,
    TransformSpec,
)
from src.domain.schemas.yaml_exporter import dump, format_feedback_prompt


def _spec() -> PipelineSpec:
    return PipelineSpec(
        schema_version="1.0",
        pipeline_id="p_test",
        name="Test",
        type="ingestion",
        owner="eng@company.com",
        schedule=ScheduleSpec(mode="cron", cron="0 6 * * *"),
        source=SourceSpec(
            asset_id="src",
            objects=[
                ExtractionSpec(
                    object_id="orders",
                    load_strategy="incremental",
                    watermark_column="updated_at",
                    page_size=500,
                    compression="snappy",
                    encoding="utf-8",
                )
            ],
        ),
        destination=DestinationSpec(
            asset_id="dst",
            objects=[DestinationObjectSpec(object_id="orders", create_if_not_exists=True)],
        ),
        transform=TransformSpec(engine="none"),
        compute=ComputeSpec(
            engine="default",
            num_workers=2,
            machine_type="n1-standard-4",
            staging_bucket="gs://test-bucket",
        ),
        quality=QualitySpec(metrics=[]),
        airflow=AirflowSpec(retries=2, sla_minutes=60),
        discovery_task=DiscoveryTaskSpec(),
    )


def test_dump_returns_valid_yaml() -> None:
    assert isinstance(yaml.safe_load(dump(_spec())), dict)


def test_dump_schema_version_is_first_key() -> None:
    assert list(yaml.safe_load(dump(_spec())).keys())[0] == "schema_version"


def test_dump_pipeline_nested_correctly() -> None:
    p = yaml.safe_load(dump(_spec()))["pipeline"]
    assert p["id"] == "p_test"
    assert p["owner"] == "eng@company.com"


def test_dump_schedule_section() -> None:
    p = yaml.safe_load(dump(_spec()))["pipeline"]
    assert p["schedule"]["mode"] == "cron"
    assert p["schedule"]["cron"] == "0 6 * * *"


def test_dump_watermark_column_present_when_set() -> None:
    p = yaml.safe_load(dump(_spec()))["pipeline"]
    assert p["source"]["objects"][0]["watermark_column"] == "updated_at"


def test_dump_optional_fields_omitted_when_none() -> None:
    plain = PipelineSpec(
        schema_version="1.0",
        pipeline_id="p",
        name="P",
        type="ingestion",
        owner="e@c.com",
        schedule=ScheduleSpec(mode="cron", cron="0 6 * * *"),
        source=SourceSpec(
            asset_id="s",
            objects=[
                ExtractionSpec(
                    object_id="t",
                    load_strategy="full_load",
                    page_size=1000,
                    compression="snappy",
                    encoding="utf-8",
                )
            ],
        ),
        destination=DestinationSpec(
            asset_id="d", objects=[DestinationObjectSpec(object_id="t", create_if_not_exists=True)]
        ),
        transform=TransformSpec(engine="none"),
        compute=ComputeSpec(
            engine="default", num_workers=1, machine_type="n1-standard-2", staging_bucket="gs://b"
        ),
        quality=QualitySpec(metrics=[]),
        airflow=AirflowSpec(),
        discovery_task=DiscoveryTaskSpec(),
    )
    obj = yaml.safe_load(dump(plain))["pipeline"]["source"]["objects"][0]
    assert "watermark_column" not in obj
    assert "sensor" not in obj
    assert "extraction_query" not in obj


def test_format_feedback_prompt_contains_iteration() -> None:
    prompt = format_feedback_prompt(errors=["owner missing @", "cron invalid"], iteration=1)
    assert "iteration 1" in prompt.lower()
    assert "owner missing @" in prompt


def test_format_feedback_prompt_includes_all_errors() -> None:
    errors = ["err_a", "err_b", "err_c"]
    prompt = format_feedback_prompt(errors=errors, iteration=2)
    for e in errors:
        assert e in prompt
