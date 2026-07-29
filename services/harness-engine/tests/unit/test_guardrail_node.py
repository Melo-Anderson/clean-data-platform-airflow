"""Tests for guardrail_node — deterministic, non-LLM validation."""

from src.application.graph.nodes.guardrail_node import guardrail_node
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
    SourceSpec,
    TransformSpec,
)


def _state(spec: PipelineSpec, context: dict | None = None) -> dict:  # type: ignore[type-arg]
    return {
        "messages": [],
        "user_prompt": "test",
        "context": context or {"avg_volume_gb": 5.0, "avg_duration_seconds": 120.0},
        "pipeline_spec": spec,
        "generated_yaml": None,
        "validation_errors": [],
        "iteration_count": 0,
        "status": "in_progress",
    }


def _spec(**kw) -> PipelineSpec:  # type: ignore[no-untyped-def]
    defaults = dict(
        schema_version="1.0",
        pipeline_id="p",
        name="T",
        type="ingestion",
        owner="eng@company.com",
        schedule=ScheduleSpec(mode="cron", cron="0 6 * * *"),
        source=SourceSpec(
            asset_id="src",
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
            asset_id="dst",
            objects=[DestinationObjectSpec(object_id="t", create_if_not_exists=True)],
        ),
        transform=TransformSpec(engine="none"),
        compute=ComputeSpec(
            engine="default", num_workers=1, machine_type="n1-standard-2", staging_bucket="gs://b"
        ),
        quality=QualitySpec(metrics=[QualityRuleSpec(type="row_count_min", value=1)]),
        airflow=AirflowSpec(),
        discovery_task=DiscoveryTaskSpec(),
    )
    defaults.update(kw)
    return PipelineSpec(**defaults)


def test_guardrail_passes_valid_spec() -> None:
    result = guardrail_node(_state(_spec()))
    assert result["validation_errors"] == []
    assert result["iteration_count"] == 1


def test_guardrail_increments_iteration_count() -> None:
    s = _state(_spec())
    s["iteration_count"] = 2
    assert guardrail_node(s)["iteration_count"] == 3


def test_rejects_default_engine_for_large_volume() -> None:
    spec = _spec(
        compute=ComputeSpec(
            engine="default", num_workers=1, machine_type="n1-standard-2", staging_bucket="gs://b"
        )
    )
    result = guardrail_node(_state(spec, {"avg_volume_gb": 150.0, "avg_duration_seconds": 3600.0}))
    assert any("compute" in e.lower() or "volume" in e.lower() for e in result["validation_errors"])


def test_accepts_spark_for_large_volume() -> None:
    spec = _spec(
        compute=ComputeSpec(
            engine="spark", num_workers=8, machine_type="n1-standard-8", staging_bucket="gs://b"
        )
    )
    result = guardrail_node(_state(spec, {"avg_volume_gb": 150.0, "avg_duration_seconds": 3600.0}))
    assert result["validation_errors"] == []


def test_rejects_overprovisioned_compute_for_small_volume() -> None:
    spec = _spec(
        compute=ComputeSpec(
            engine="spark", num_workers=16, machine_type="n1-standard-16", staging_bucket="gs://b"
        )
    )
    result = guardrail_node(_state(spec, {"avg_volume_gb": 2.0, "avg_duration_seconds": 30.0}))
    assert any("worker" in e.lower() or "compute" in e.lower() for e in result["validation_errors"])


def test_fails_pii_columns_without_quality_rules() -> None:
    spec = _spec(quality=QualitySpec(metrics=[]))
    ctx = {"avg_volume_gb": 5.0, "avg_duration_seconds": 120.0, "pii_columns": ["cpf", "name"]}
    result = guardrail_node(_state(spec, ctx))
    assert any("pii" in e.lower() or "governance" in e.lower() for e in result["validation_errors"])


def test_fails_empty_staging_bucket() -> None:
    s = _state(_spec())
    s["pipeline_spec"] = _spec().model_copy(
        update={
            "compute": ComputeSpec(
                engine="default", num_workers=1, machine_type="n1-standard-2", staging_bucket="   "
            )
        }
    )
    result = guardrail_node(s)
    assert any("bucket" in e.lower() or "staging" in e.lower() for e in result["validation_errors"])
