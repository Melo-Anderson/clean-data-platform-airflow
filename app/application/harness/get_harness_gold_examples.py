from __future__ import annotations

from typing import Any

import yaml

from app.application.unit_of_work import UnitOfWork
from app.infrastructure.yaml_generator.pipeline_yaml_generator import PipelineYamlGenerator

# Canonical fallback YAMLs per pipeline type (no quality.metrics block).
_FALLBACK_YAMLS: dict[str, dict[str, Any]] = {
    "ingestion": {
        "schema_version": "1.0",
        "pipeline_id": "p_canonical_ingestion",
        "name": "Canonical Ingestion Pipeline",
        "type": "ingestion",
        "owner": "eng@company.com",
        "schedule": {"mode": "cron", "cron": "0 6 * * *"},
        "source": {
            "asset_id": "src_example",
            "objects": [
                {
                    "object_id": "example_table",
                    "load_strategy": "incremental",
                    "watermark_column": "updated_at",
                    "page_size": 5000,
                    "compression": "snappy",
                    "encoding": "utf-8",
                }
            ],
        },
        "destination": {
            "asset_id": "dest_dwh",
            "objects": [{"object_id": "example_table", "create_if_not_exists": True}],
        },
        "transform": {"engine": "none"},
        "compute": {
            "engine": "default",
            "config": {"num_workers": 2, "machine_type": "n1-standard-2"},
            "staging_bucket": "gs://my-bucket/staging",
        },
        "airflow": {
            "retries": 3,
            "retry_delay_minutes": 5,
            "execution_timeout_minutes": 120,
            "sla_minutes": 90,
            "tags": ["ingestion"],
            "pool": "default_pool",
        },
        "discovery_task": {"enabled": True, "on_critical_change": "warn"},
    },
    "etl": {
        "schema_version": "1.0",
        "pipeline_id": "p_canonical_etl",
        "name": "Canonical ETL Pipeline",
        "type": "etl",
        "owner": "analytics@company.com",
        "schedule": {
            "mode": "trigger_with_gate",
            "cron": "0 8 * * *",
            "depends_on": [
                {
                    "pipeline_id": "p_canonical_ingestion",
                    "dependency_type": "dataset",
                    "require_same_day": True,
                }
            ],
        },
        "source": {
            "asset_id": "dest_dwh",
            "objects": [
                {
                    "object_id": "example_table",
                    "load_strategy": "incremental",
                    "watermark_column": "updated_at",
                    "page_size": 10000,
                    "compression": "snappy",
                    "encoding": "utf-8",
                }
            ],
        },
        "destination": {
            "asset_id": "dest_dwh",
            "objects": [{"object_id": "example_agg", "create_if_not_exists": True}],
        },
        "transform": {"engine": "dbt", "ref": "marts/example_agg"},
        "compute": {
            "engine": "spark",
            "config": {"num_workers": 4, "machine_type": "n1-standard-4"},
            "staging_bucket": "gs://my-bucket/staging",
        },
        "airflow": {
            "retries": 2,
            "retry_delay_minutes": 10,
            "execution_timeout_minutes": 180,
            "sla_minutes": 120,
            "tags": ["etl"],
            "pool": "spark_pool",
        },
        "discovery_task": {"enabled": True, "on_critical_change": "fail"},
    },
    "export": {
        "schema_version": "1.0",
        "pipeline_id": "p_canonical_export",
        "name": "Canonical Export Pipeline",
        "type": "export",
        "owner": "data-ops@company.com",
        "schedule": {
            "mode": "trigger",
            "depends_on": [
                {
                    "pipeline_id": "p_canonical_etl",
                    "dependency_type": "dataset",
                    "require_same_day": True,
                }
            ],
        },
        "source": {
            "asset_id": "dest_dwh",
            "objects": [
                {
                    "object_id": "example_agg",
                    "load_strategy": "full_load",
                    "page_size": 50000,
                    "compression": "snappy",
                    "encoding": "utf-8",
                }
            ],
        },
        "destination": {
            "asset_id": "partner_sftp",
            "objects": [{"object_id": "report_csv", "create_if_not_exists": True}],
        },
        "transform": {"engine": "none"},
        "compute": {
            "engine": "default",
            "config": {"num_workers": 1, "machine_type": "n1-standard-2"},
            "staging_bucket": "gs://my-bucket/staging",
        },
        "airflow": {
            "retries": 1,
            "retry_delay_minutes": 5,
            "execution_timeout_minutes": 60,
            "sla_minutes": 45,
            "tags": ["export"],
            "pool": "default_pool",
        },
        "discovery_task": {"enabled": False, "on_critical_change": "ignore"},
    },
}


class GetHarnessGoldExamplesUseCase:
    """Return real or canonical pipeline YAML examples for the Harness Engine."""

    def __init__(
        self,
        uow: UnitOfWork | None = None,
        yaml_generator: PipelineYamlGenerator | None = None,
    ) -> None:
        self._uow = uow
        self._yaml_generator = yaml_generator or PipelineYamlGenerator()

    async def execute(
        self,
        pipeline_type: str,
        compute_engine: str | None = None,
        transform_engine: str | None = None,
        source_asset_id: str | None = None,
        limit: int = 3,
    ) -> dict[str, Any]:
        """Return up to `limit` YAML examples for the given pipeline type.

        Args:
            pipeline_type: Required. One of 'ingestion', 'etl', 'export'.
            compute_engine: Optional. Filter by compute engine (e.g. 'spark', 'duckdb').
            transform_engine: Optional. Filter by transform engine (e.g. 'dbt').
            source_asset_id: Optional. Filter by source asset ID.
            limit: Maximum number of examples to return. Default 3.

        Returns:
            Dict with keys: pipeline_type, total_count, examples (list of {pipeline_id, yaml_snippet}).
        """
        examples: list[dict[str, str]] = []

        if self._uow:
            pipelines = await self._uow.pipelines.find_all()
            filtered = [
                p
                for p in pipelines
                if p.type.value == pipeline_type
                and (not compute_engine or p.compute.engine.value == compute_engine)
                and (not transform_engine or p.transform.engine.value == transform_engine)
                and (not source_asset_id or p.source_asset == source_asset_id)
            ][:limit]
            for p in filtered:
                yaml_snippet = self._yaml_generator.generate(p)
                examples.append({"pipeline_id": p.id, "yaml_snippet": yaml_snippet})

        if not examples:
            # Use full canonical fallback (no quality.metrics) for the requested type.
            fallback = _FALLBACK_YAMLS.get(pipeline_type, _FALLBACK_YAMLS["ingestion"])
            examples.append(
                {
                    "pipeline_id": str(fallback["pipeline_id"]),
                    "yaml_snippet": yaml.dump(fallback, sort_keys=False, allow_unicode=True),
                }
            )

        return {
            "pipeline_type": pipeline_type,
            "total_count": len(examples),
            "examples": examples[:limit],
        }
