from __future__ import annotations

import os
import re
import subprocess
from datetime import UTC, datetime
from typing import Any

import yaml
from jinja2 import Environment, FileSystemLoader

from app.domain.pipelines.pipeline_type import PipelineType

VALID_PIPELINE_TYPES = {e.value for e in PipelineType}
_TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")


def _normalize_paths(data: Any) -> Any:
    """Recursively normalize Windows backslashes in strings to forward slashes."""
    if isinstance(data, str):
        if "\\" in data:
            return data.replace("\\", "/")
        return data
    if isinstance(data, dict):
        return {k: _normalize_paths(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_normalize_paths(item) for item in data]
    return data


def _canonicalize_pipeline_dict(raw: dict[str, Any]) -> dict[str, Any]:
    """Canonicalize pipeline dictionary for clean Jinja2 template rendering."""
    p = dict(raw)

    source_val = p.get("source")
    source_dict: dict[str, Any] = source_val if isinstance(source_val, dict) else {}
    dest_val = p.get("destination")
    dest_dict: dict[str, Any] = dest_val if isinstance(dest_val, dict) else {}
    sched_val = p.get("schedule")
    sched_dict: dict[str, Any] = sched_val if isinstance(sched_val, dict) else {}
    airflow_val = p.get("airflow")
    airflow_dict: dict[str, Any] = airflow_val if isinstance(airflow_val, dict) else {}
    quality_val = p.get("quality")
    quality_dict: dict[str, Any] = quality_val if isinstance(quality_val, dict) else {}
    compute_val = p.get("compute")
    compute_dict: dict[str, Any] = compute_val if isinstance(compute_val, dict) else {}
    discovery_task_val = p.get("discovery_task")
    discovery_task_dict: dict[str, Any] = (
        discovery_task_val if isinstance(discovery_task_val, dict) else {}
    )

    src_asset_name = str(source_dict.get("asset_name") or "")
    dest_asset_name = str(dest_dict.get("asset_name") or "")
    source_objects = list(source_dict.get("objects") or [])
    destination_objects = list(dest_dict.get("objects") or [])

    p["source_asset_name"] = src_asset_name
    p["destination_asset_name"] = dest_asset_name
    p["source_objects"] = source_objects
    p["destination_objects"] = destination_objects
    p["source"] = {"asset_name": src_asset_name, "objects": source_objects}
    p["destination"] = {"asset_name": dest_asset_name, "objects": destination_objects}

    dest_assets = [dest_asset_name] if dest_asset_name else []
    p["destination_assets"] = dest_assets

    if dest_assets:
        p["outlets"] = [f"platform://asset/{a}" for a in dest_assets]
    elif src_asset_name:
        p["outlets"] = [f"platform://asset/{src_asset_name}"]
    else:
        p["outlets"] = [f"platform://pipeline/{p.get('id', '')}"]

    effective_asset = dest_asset_name or src_asset_name
    if effective_asset:
        p["asset_uri"] = f"platform://asset/{effective_asset}"
    else:
        p["asset_uri"] = f"platform://pipeline/{p.get('id', '')}"

    depends_on = list(sched_dict.get("depends_on") or [])
    upstream = []
    if depends_on:
        upstream.extend(
            [
                dep.get("asset_uri") or f"platform://asset/{dep['pipeline_id']}"
                for dep in depends_on
                if dep.get("dependency_type") == "dataset" or "pipeline_id" in dep
            ]
        )
    elif src_asset_name and not sched_dict.get("cron"):
        upstream.append(f"platform://asset/{src_asset_name}")

    p["upstream_assets"] = upstream
    p["schedule"] = {
        "mode": sched_dict.get("mode", "cron" if sched_dict.get("cron") else "event"),
        "cron": sched_dict.get("cron", ""),
        "depends_on": depends_on,
    }
    p["airflow"] = {
        "retries": airflow_dict.get("retries", 3),
        "retry_delay_minutes": airflow_dict.get("retry_delay_minutes", 5),
        "execution_timeout_minutes": airflow_dict.get("execution_timeout_minutes", 120),
        "sla_minutes": airflow_dict.get("sla_minutes", 90),
        "tags": list(airflow_dict.get("tags") or [p.get("type", "pipeline"), p.get("name", "")]),
        "pool": airflow_dict.get("pool", "default_pool"),
    }
    engine = compute_dict.get("engine") or (
        "dbt" if p.get("type") == "transformation" else "default"
    )

    default_staging = (
        os.environ.get("PLATFORM_COMPUTE__DBT_STAGING_BUCKET") or "/opt/airflow/logs/dbt_outputs"
        if engine == "dbt"
        else (
            os.environ.get("PLATFORM_COMPUTE__TRANSFORMATION_STAGING_BUCKET")
            or "/opt/airflow/logs/transformation_outputs"
        )
    )
    staging_bucket = compute_dict.get("staging_bucket") or default_staging
    compute_config = dict(compute_dict.get("config", {}))

    p["compute"] = {
        "engine": engine,
        "staging_bucket": staging_bucket,
        "select": compute_dict.get("select", ""),
        "config": compute_config,
    }
    p["quality"] = {
        "metrics": list(quality_dict.get("metrics") or []),
    }
    p["discovery_task"] = {
        "enabled": discovery_task_dict.get("enabled", True),
        "on_critical_change": discovery_task_dict.get("on_critical_change", "block"),
    }

    return p


def _resolve_commit_hash(explicit: str | None = None) -> str:
    if explicit:
        return explicit

    build_commit = os.environ.get("PLATFORM_BUILD_COMMIT_HASH")
    if build_commit and build_commit != "unknown":
        return build_commit

    try:
        return (
            subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                stderr=subprocess.DEVNULL,
                timeout=2,
            )
            .decode()
            .strip()
        )
    except Exception:
        return "unknown"


class DagGenerator:
    """Generates Airflow 3 Python DAG code from Pipeline YAML definition."""

    def __init__(self, commit_hash: str | None = None) -> None:
        self._commit_hash = _resolve_commit_hash(commit_hash)
        self._env = Environment(
            loader=FileSystemLoader(_TEMPLATES_DIR),
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True,
        )
        self._env.filters["sanitize_identifier"] = lambda s: "".join(
            c if c.isalnum() or c == "_" else "_" for c in str(s)
        )
        self._env.filters["sanitize_dag_id"] = lambda s: re.sub(r"[^a-zA-Z0-9_.-]+", "_", str(s))

    def generate(self, pipeline_yaml: str) -> str:
        pipeline_dict = yaml.safe_load(pipeline_yaml)
        pipeline_config = pipeline_dict.get("pipeline", pipeline_dict)
        return self.render_pipeline_config(pipeline_config)

    def generate_transformation_dag(self, pipeline_config: dict[str, Any]) -> str:
        return self.render_pipeline_config(pipeline_config, default_type="transformation")

    def render_pipeline_config(
        self, pipeline_config: dict[str, Any], default_type: str = "ingestion"
    ) -> str:
        canonical_config = _canonicalize_pipeline_dict(pipeline_config)
        normalized_config = _normalize_paths(canonical_config)
        pipeline_type = (
            normalized_config.get("type") or normalized_config.get("pipeline_type") or default_type
        )
        if pipeline_type not in VALID_PIPELINE_TYPES:
            raise ValueError(
                f"Unknown pipeline type: {pipeline_type!r}. Valid types: {sorted(VALID_PIPELINE_TYPES)}."
            )
        template_name = f"{pipeline_type}_dag.py.j2"

        template = self._env.get_template(template_name)
        now = datetime.now(tz=UTC).isoformat()

        return template.render(
            pipeline=normalized_config,
            template_version="1.0.0",
            generated_at=now,
            commit_hash=self._commit_hash,
        )
