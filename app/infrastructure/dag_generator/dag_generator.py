from __future__ import annotations

import os
import re
from datetime import UTC, datetime
from typing import Any

import yaml
from jinja2 import Environment, FileSystemLoader

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
    """Canonicalize and flatten pipeline dictionary for clean Jinja2 template rendering."""
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

    src_asset = source_dict.get("asset") or p.get("source_asset") or ""
    dest_asset = dest_dict.get("asset") or p.get("destination_asset") or ""

    p["source_asset"] = src_asset
    p["destination_asset"] = dest_asset
    p["source_objects"] = source_dict.get("objects") or p.get("source_objects") or []
    p["destination_objects"] = dest_dict.get("objects") or p.get("destination_objects") or []

    dest_assets = p.get("destination_assets") or ([dest_asset] if dest_asset else [])
    p["destination_assets"] = dest_assets

    if dest_assets:
        p["outlets"] = [f"platform://asset/{a}" for a in dest_assets]
    elif src_asset:
        p["outlets"] = [f"platform://asset/{src_asset}"]
    else:
        p["outlets"] = [f"platform://pipeline/{p.get('id', '')}"]

    if dest_asset:
        p["asset_uri"] = f"platform://asset/{dest_asset}"
    elif dest_assets:
        p["asset_uri"] = f"platform://asset/{dest_assets[0]}"
    elif src_asset:
        p["asset_uri"] = f"platform://asset/{src_asset}"
    else:
        p["asset_uri"] = f"platform://pipeline/{p.get('id', '')}"

    depends_on = sched_dict.get("depends_on") or p.get("depends_on") or []
    upstream = []
    if depends_on:
        upstream.extend(
            [
                dep.get("asset_uri") or f"platform://asset/{dep['pipeline_id']}"
                for dep in depends_on
                if dep.get("dependency_type") == "dataset" or "pipeline_id" in dep
            ]
        )
    elif src_asset and not sched_dict.get("cron") and not p.get("cron_schedule"):
        upstream.append(f"platform://asset/{src_asset}")

    p["upstream_assets"] = upstream
    p["schedule"] = {
        "mode": sched_dict.get(
            "mode", "cron" if (sched_dict.get("cron") or p.get("cron_schedule")) else "event"
        ),
        "cron": sched_dict.get("cron") or p.get("cron_schedule") or "",
        "depends_on": depends_on,
    }
    p["airflow"] = {
        "retries": airflow_dict.get("retries", p.get("retries", 3)),
        "retry_delay_minutes": airflow_dict.get(
            "retry_delay_minutes", p.get("retry_delay_minutes", 5)
        ),
        "execution_timeout_minutes": airflow_dict.get(
            "execution_timeout_minutes", p.get("execution_timeout_minutes", 120)
        ),
        "sla_minutes": airflow_dict.get("sla_minutes", p.get("sla_minutes", 90)),
        "tags": list(airflow_dict.get("tags") or [p.get("type", "pipeline"), p.get("name", "")]),
        "pool": airflow_dict.get("pool", "default_pool"),
    }
    p["compute"] = {
        "engine": compute_dict.get("engine", p.get("compute_engine", "default")),
        "staging_bucket": compute_dict.get("staging_bucket", "/opt/airflow/logs/dbt_outputs"),
        "select": compute_dict.get("select")
        or compute_dict.get("config", {}).get("select")
        or p.get("select_models", ""),
        "config": compute_dict.get("config", {}),
    }
    p["quality"] = {
        "metrics": quality_dict.get("metrics") or p.get("quality_rules") or [],
    }

    return p


class DagGenerator:
    """Generates Airflow 3 Python DAG code from Pipeline YAML definition."""

    def __init__(self) -> None:
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
        template_name = f"{pipeline_type}_dag.py.j2"

        template = self._env.get_template(template_name)
        now = datetime.now(tz=UTC).isoformat()

        return template.render(
            pipeline=normalized_config,
            template_version="1.0.0",
            generated_at=now,
            commit_hash="local",
        )
