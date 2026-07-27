"""Integration tests for the full LangGraph workflow. LLM replaced by MagicMock."""

from unittest.mock import MagicMock

from src.application.graph.state import initial_state
from src.application.graph.workflow import build_graph
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


class Stub:
    def get_object_metadata(self, *_):  # type: ignore[no-untyped-def]
        return None

    def list_objects_for_asset(self, *_):  # type: ignore[no-untyped-def]
        return []

    def get_execution_metrics(self, *_):  # type: ignore[no-untyped-def]
        return None


def _spec(**kw) -> PipelineSpec:  # type: ignore[no-untyped-def]
    base = dict(
        schema_version="1.0",
        pipeline_id="p_int",
        name="Integration",
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
            engine="default", num_workers=2, machine_type="n1-standard-4", staging_bucket="gs://b"
        ),
        quality=QualitySpec(metrics=[QualityRuleSpec(type="row_count_min", value=1)]),
        airflow=AirflowSpec(),
        discovery_task=DiscoveryTaskSpec(),
    )
    base.update(kw)
    return PipelineSpec(**base)


def _mock(spec: PipelineSpec) -> MagicMock:
    m, s = MagicMock(), MagicMock()
    s.invoke.return_value = spec
    m.with_structured_output.return_value = s
    return m


def test_approved_on_valid_spec() -> None:
    graph = build_graph(Stub(), Stub(), llm=_mock(_spec()))  # type: ignore[arg-type]
    result = graph.invoke(initial_state("Ingest sales"))
    assert result["status"] == "approved"
    assert result["generated_yaml"] is not None
    assert result["validation_errors"] == []
    assert result["iteration_count"] == 1


def test_retries_on_guardrail_failure_then_succeeds() -> None:
    bad = _spec(
        compute=ComputeSpec(
            engine="default", num_workers=1, machine_type="n1-standard-2", staging_bucket="gs://b"
        )
    )
    good = _spec(
        compute=ComputeSpec(
            engine="spark", num_workers=8, machine_type="n1-standard-8", staging_bucket="gs://b"
        )
    )
    m, s = MagicMock(), MagicMock()
    s.invoke.side_effect = [bad, good]
    m.with_structured_output.return_value = s
    graph = build_graph(Stub(), Stub(), llm=m)  # type: ignore[arg-type]
    state = initial_state("Ingest 150 GB table")
    state["context"] = {
        "avg_volume_gb": 150.0,
        "avg_duration_seconds": 3600.0,
        "few_shot_examples": [],
        "platform_rules": "",
    }
    result = graph.invoke(state)
    assert result["status"] == "approved"
    assert result["iteration_count"] == 2


def test_fails_after_max_iterations() -> None:
    bad = _spec(
        compute=ComputeSpec(
            engine="default", num_workers=1, machine_type="n1-standard-2", staging_bucket="gs://b"
        )
    )
    m, s = MagicMock(), MagicMock()
    s.invoke.return_value = bad
    m.with_structured_output.return_value = s
    graph = build_graph(Stub(), Stub(), llm=m)  # type: ignore[arg-type]
    state = initial_state("Ingest 200 GB warehouse")
    state["context"] = {
        "avg_volume_gb": 200.0,
        "avg_duration_seconds": 7200.0,
        "few_shot_examples": [],
        "platform_rules": "",
    }
    result = graph.invoke(state)
    assert result["status"] == "failed_max_iterations"
    assert result["iteration_count"] == 3
