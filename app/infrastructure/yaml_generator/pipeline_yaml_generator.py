from __future__ import annotations

import yaml

from app.domain.pipelines.pipeline import Pipeline


class PipelineYamlGenerator:
    """
    Generates YAML configuration string from a Pipeline domain entity.
    Single source of truth for YAML output — no hand-editing of YAML.
    """

    def generate(self, pipeline: Pipeline) -> str:
        return yaml.dump(
            self._build_dict(pipeline),
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        )

    def _build_dict(self, p: Pipeline) -> dict:
        return {
            "schema_version": p.schema_version,
            "pipeline": {
                "id": p.id,
                "name": p.name,
                "type": p.type.value,
                "owner": p.owner.value,
                "schedule": self._schedule_dict(p),
                "source": self._source_dict(p),
                "destination": self._destination_dict(p),
                "transform": self._transform_dict(p),
                "compute": self._compute_dict(p),
                "quality": self._quality_dict(p),
                "airflow": self._airflow_dict(p),
                "discovery_task": {
                    "enabled": p.discovery_task.enabled,
                    "on_critical_change": p.discovery_task.on_critical_change.value,
                },
            },
        }

    def _schedule_dict(self, p: Pipeline) -> dict:
        s = p.schedule
        d: dict = {"mode": s.mode.value}
        if s.cron_schedule:
            d["cron"] = s.cron_schedule.expression
        if s.depends_on:
            d["depends_on"] = [
                {
                    "pipeline_id": dep.pipeline_id,
                    "require_same_day": dep.require_same_day,
                    "dependency_type": dep.dependency_type.value,
                }
                for dep in s.depends_on
            ]
        return d

    def _source_dict(self, p: Pipeline) -> dict:
        objects = []
        for ext in p.source_objects:
            obj: dict = {
                "object_id": ext.object_id,
                "load_strategy": ext.load_strategy.value,
                "page_size": ext.page_size,
                "compression": ext.compression,
                "encoding": ext.encoding,
            }
            if ext.watermark_column:
                obj["watermark_column"] = ext.watermark_column
            if ext.partition_column:
                obj["partition_column"] = ext.partition_column
            if ext.extraction_query:
                obj["extraction_query"] = ext.extraction_query
            if ext.sensor:
                obj["sensor"] = {
                    "query": ext.sensor.query,
                    "timeout_minutes": ext.sensor.timeout_minutes,
                    "poke_interval_seconds": ext.sensor.poke_interval_seconds,
                }
            objects.append(obj)
        return {"asset_id": p.source_asset_id, "objects": objects}

    def _destination_dict(self, p: Pipeline) -> dict:
        return {
            "asset_id": p.destination_asset_id,
            "objects": [
                {"object_id": d.object_id, "create_if_not_exists": d.create_if_not_exists}
                for d in p.destination_objects
            ],
        }

    def _transform_dict(self, p: Pipeline) -> dict:
        t = p.transform
        d: dict = {"engine": t.engine.value}
        if t.ref:
            d["ref"] = t.ref
        return d

    def _compute_dict(self, p: Pipeline) -> dict:
        c = p.compute
        return {
            "engine": c.engine.value,
            "staging_bucket": c.staging_bucket,
            "config": {"num_workers": c.num_workers, "machine_type": c.machine_type},
        }

    def _quality_dict(self, p: Pipeline) -> dict:
        metrics = []
        for r in p.quality_rules:
            rule: dict = {"type": r.type.value}
            if r.column:
                rule["column"] = r.column
            if r.value is not None:
                rule["value"] = r.value
            metrics.append(rule)
        return {"metrics": metrics}

    def _airflow_dict(self, p: Pipeline) -> dict:
        a = p.airflow
        return {
            "retries": a.retries,
            "retry_delay_minutes": a.retry_delay_minutes,
            "execution_timeout_minutes": a.execution_timeout_minutes,
            "sla_minutes": a.sla_minutes,
            "tags": list(a.tags),
            "pool": a.pool,
        }
