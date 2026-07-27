"""
YAML Exporter — converts PipelineSpec to canonical YAML string.

Rules:
- schema_version is always first key (sort_keys=False preserves insertion order)
- Optional fields (watermark_column, sensor, etc.) are omitted when None
- Key order mirrors PipelineYamlGenerator exactly for platform parity
"""

from __future__ import annotations

import yaml

from src.domain.schemas.pipeline_spec import ExtractionSpec, PipelineSpec


def dump(spec: PipelineSpec) -> str:
    """Serialize PipelineSpec to clean YAML. schema_version is always first key."""
    return yaml.dump(
        _build_dict(spec),
        default_flow_style=False,
        allow_unicode=True,
        sort_keys=False,
    )


def format_feedback_prompt(errors: list[str], iteration: int) -> str:
    """
    Feedback prefix injected into LLM prompt on retry iterations (iteration_count > 0).
    Must be clear, numbered, and actionable.
    """
    numbered = "\n".join(f"  {i + 1}. {e}" for i, e in enumerate(errors))
    return (
        f"Iteration {iteration} — YAML Harness Feedback\n"
        f"The previously generated YAML failed validation with the following errors:\n"
        f"{numbered}\n\n"
        f"Fix ALL errors listed above. Do not remove or alter fields not listed as errors."
    )


def _build_dict(spec: PipelineSpec) -> dict:  # type: ignore[type-arg]
    return {
        "schema_version": spec.schema_version,
        "pipeline": {
            "id": spec.pipeline_id,
            "name": spec.name,
            "type": spec.type,
            "owner": spec.owner,
            "schedule": _schedule_dict(spec),
            "source": _source_dict(spec),
            "destination": _destination_dict(spec),
            "transform": _transform_dict(spec),
            "compute": _compute_dict(spec),
            "quality": _quality_dict(spec),
            "airflow": _airflow_dict(spec),
            "discovery_task": {
                "enabled": spec.discovery_task.enabled,
                "on_critical_change": spec.discovery_task.on_critical_change,
            },
        },
    }


def _schedule_dict(spec: PipelineSpec) -> dict:  # type: ignore[type-arg]
    s = spec.schedule
    d: dict = {"mode": s.mode}  # type: ignore[type-arg]
    if s.cron:
        d["cron"] = s.cron
    if s.depends_on:
        d["depends_on"] = [
            {
                "pipeline_id": dep.pipeline_id,
                "require_same_day": dep.require_same_day,
                "dependency_type": dep.dependency_type,
            }
            for dep in s.depends_on
        ]
    return d


def _source_dict(spec: PipelineSpec) -> dict:  # type: ignore[type-arg]
    return {
        "asset_id": spec.source.asset_id,
        "objects": [_extraction_dict(o) for o in spec.source.objects],
    }


def _extraction_dict(ext: ExtractionSpec) -> dict:  # type: ignore[type-arg]
    obj: dict = {  # type: ignore[type-arg]
        "object_id": ext.object_id,
        "load_strategy": ext.load_strategy,
        "page_size": ext.page_size,
        "compression": ext.compression,
        "encoding": ext.encoding,
    }
    if ext.watermark_column is not None:
        obj["watermark_column"] = ext.watermark_column
    if ext.partition_column is not None:
        obj["partition_column"] = ext.partition_column
    if ext.extraction_query is not None:
        obj["extraction_query"] = ext.extraction_query
    if ext.sensor is not None:
        obj["sensor"] = {
            "query": ext.sensor.query,
            "timeout_minutes": ext.sensor.timeout_minutes,
            "poke_interval_seconds": ext.sensor.poke_interval_seconds,
        }
    return obj


def _destination_dict(spec: PipelineSpec) -> dict:  # type: ignore[type-arg]
    return {
        "asset_id": spec.destination.asset_id,
        "objects": [
            {"object_id": d.object_id, "create_if_not_exists": d.create_if_not_exists}
            for d in spec.destination.objects
        ],
    }


def _transform_dict(spec: PipelineSpec) -> dict:  # type: ignore[type-arg]
    d: dict = {"engine": spec.transform.engine}  # type: ignore[type-arg]
    if spec.transform.ref:
        d["ref"] = spec.transform.ref
    return d


def _compute_dict(spec: PipelineSpec) -> dict:  # type: ignore[type-arg]
    c = spec.compute
    return {
        "engine": c.engine,
        "staging_bucket": c.staging_bucket,
        "config": {"num_workers": c.num_workers, "machine_type": c.machine_type},
    }


def _quality_dict(spec: PipelineSpec) -> dict:  # type: ignore[type-arg]
    metrics = []
    for r in spec.quality.metrics:
        entry: dict = {"type": r.type}  # type: ignore[type-arg]
        if r.column is not None:
            entry["column"] = r.column
        if r.value is not None:
            entry["value"] = r.value
        metrics.append(entry)
    return {"metrics": metrics}


def _airflow_dict(spec: PipelineSpec) -> dict:  # type: ignore[type-arg]
    a = spec.airflow
    return {
        "retries": a.retries,
        "retry_delay_minutes": a.retry_delay_minutes,
        "execution_timeout_minutes": a.execution_timeout_minutes,
        "sla_minutes": a.sla_minutes,
        "tags": a.tags,
        "pool": a.pool,
    }
