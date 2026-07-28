"""
Context Node (Node 1) — Feed-Forward & Context Engineering.

Resolves AnalyticsContext via injected MetadataPort and MetricsPort.
Injects:
- Aggregated metrics (avg_volume_gb, avg_duration_seconds)
- PII columns list (for guardrail governance enforcement)
- Curated few-shot YAML examples (anchor LLM structured output)
- Platform rules summary (prevents most common LLM errors)

Context engineering rule: use AGGREGATED metrics only — no raw log rows.
"""

from __future__ import annotations

from typing import Any

from src.domain.ports import MetadataPort, MetricsPort, PlatformExamplesPort, PlatformSchemaPort


def make_context_node(
    metadata_port: MetadataPort,
    metrics_port: MetricsPort,
    schema_port: PlatformSchemaPort | None = None,
    examples_port: PlatformExamplesPort | None = None,
) -> Any:
    """Factory: returns context_node closed over injected ports."""

    def context_node(state: dict[str, Any]) -> dict[str, Any]:
        existing_ctx = state.get("context", {})
        context: dict[str, Any] = {
            "user_prompt": state.get("user_prompt", ""),
            "schema_metadata": existing_ctx.get("schema_metadata", []),
            "avg_volume_gb": existing_ctx.get("avg_volume_gb", 0.0),
            "avg_duration_seconds": existing_ctx.get("avg_duration_seconds", 0.0),
            "p95_duration_seconds": existing_ctx.get("p95_duration_seconds", 0.0),
            "pii_columns": existing_ctx.get("pii_columns", []),
            "few_shot_examples": existing_ctx.get("few_shot_examples", _few_shot_examples()),
            "platform_rules": existing_ctx.get("platform_rules", _platform_rules_summary()),
            # Contratos dinâmicos da plataforma
            "platform_schema": schema_port.get_json_schema() if schema_port else {},
            "gold_examples": examples_port.get_gold_examples() if examples_port else {},
        }
        return {"context": context}

    return context_node


def _few_shot_examples() -> list[dict[str, str]]:
    """Curated YAML snippets. Anchor LLM structured output and reduce iteration count."""
    return [
        {
            "type": "ingestion",
            "description": "Daily incremental ingestion from DB with watermark",
            "yaml_snippet": (
                "schedule:\n  mode: cron\n  cron: '0 6 * * *'\n"
                "source:\n  objects:\n"
                "  - load_strategy: incremental\n    watermark_column: updated_at\n"
            ),
        },
        {
            "type": "etl",
            "description": "dbt transformation triggered by upstream pipeline",
            "yaml_snippet": (
                "schedule:\n  mode: trigger\n  depends_on:\n"
                "  - pipeline_id: p_ingest_sales\n    dependency_type: dataset\n"
                "transform:\n  engine: dbt\n  ref: marts/sales_daily\n"
            ),
        },
        {
            "type": "export",
            "description": "Full export triggered with daily gate",
            "yaml_snippet": (
                "schedule:\n  mode: trigger_with_gate\n  cron: '0 8 * * *'\n"
                "  depends_on:\n  - pipeline_id: p_etl_sales\n    dependency_type: dataset\n"
            ),
        },
    ]


def _platform_rules_summary() -> str:
    return (
        "Platform YAML Rules (non-negotiable):\n"
        "- schema_version must be '1.0'\n"
        "- owner must be valid email (contains @)\n"
        "- schedule.mode=cron -> cron required, no depends_on\n"
        "- schedule.mode=trigger -> depends_on required, no cron\n"
        "- schedule.mode=trigger_with_gate -> both cron and depends_on required\n"
        "- transform.engine=dbt|dataform -> transform.ref required\n"
        "- sensor.query must be non-empty when sensor is set\n"
        "- compute.staging_bucket must be gs:// or s3:// URI\n"
        "- volume > 100 GB: engine=spark|dataflow with num_workers >= 4"
    )
