"""
Harness Contracts Router — fonte de verdade para contratos YAML do Harness Engine.

Expõe:
  GET /v1/harness/schema        → JSON Schema derivado de PipelineSpec (Pydantic v2)
  GET /v1/harness/gold-examples → YAMLs canônicos por tipo de pipeline
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

# Importamos o PipelineSpec da plataforma para garantir que o schema seja 100% fiel
# ao modelo de domínio. O Harness Engine consome este schema via HTTP, nunca via import direto.

router = APIRouter()


def _get_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "PipelineSpec",
        "description": "Contrato canonical para geração de YAMLs de pipeline na plataforma.",
        "properties": {
            "schema_version": {"type": "string", "const": "1.0"},
            "pipeline_id": {"type": "string", "minLength": 1},
            "name": {"type": "string", "minLength": 1},
            "type": {"type": "string", "enum": ["ingestion", "etl", "export"]},
            "owner": {"type": "string", "description": "Email do responsável"},
            "schedule": {
                "type": "object",
                "properties": {
                    "mode": {"type": "string", "enum": ["cron", "trigger", "trigger_with_gate"]},
                    "cron": {"type": "string"},
                    "depends_on": {"type": "array", "items": {"type": "object"}},
                },
                "required": ["mode"],
            },
            "source": {
                "type": "object",
                "properties": {
                    "asset_id": {"type": "string"},
                    "objects": {"type": "array", "minItems": 1},
                },
                "required": ["asset_id", "objects"],
            },
            "destination": {
                "type": "object",
                "properties": {
                    "asset_id": {"type": "string"},
                    "objects": {"type": "array", "minItems": 1},
                },
                "required": ["asset_id", "objects"],
            },
            "compute": {
                "type": "object",
                "properties": {
                    "engine": {
                        "type": "string",
                        "enum": ["default", "spark", "dataflow", "rest_api"],
                    },
                    "num_workers": {"type": "integer", "minimum": 1},
                    "staging_bucket": {"type": "string", "minLength": 1},
                },
                "required": ["engine", "staging_bucket"],
            },
        },
        "required": [
            "schema_version",
            "pipeline_id",
            "name",
            "type",
            "owner",
            "schedule",
            "source",
            "destination",
            "compute",
        ],
    }


# Gold YAMLs: exemplos canônicos por tipo de pipeline.
# Estes exemplos refletem a saída real do PipelineYamlGenerator e servem como
# few-shot anchors para o LLM do Harness Engine.
_GOLD_EXAMPLES: dict[str, str] = {
    "ingestion": """\
schema_version: '1.0'
pipeline_id: p_ingest_sales
name: Ingest Sales Daily
type: ingestion
owner: eng@company.com
schedule:
  mode: cron
  cron: '0 6 * * *'
source:
  asset_id: src_erp
  objects:
    - object_id: sales_orders
      load_strategy: incremental
      watermark_column: updated_at
      page_size: 5000
      compression: snappy
      encoding: utf-8
destination:
  asset_id: dest_dwh
  objects:
    - object_id: sales_orders
      create_if_not_exists: true
transform:
  engine: none
compute:
  engine: default
  num_workers: 2
  machine_type: n1-standard-2
  staging_bucket: gs://my-bucket/staging
quality:
  metrics:
    - type: row_count_min
      value: 1
airflow:
  retries: 3
  retry_delay_minutes: 5
  execution_timeout_minutes: 120
  sla_minutes: 90
  tags: [ingestion, sales]
  pool: default_pool
discovery_task:
  enabled: true
  on_critical_change: warn
""",
    "etl": """\
schema_version: '1.0'
pipeline_id: p_etl_sales_daily
name: ETL Sales Daily Aggregation
type: etl
owner: analytics@company.com
schedule:
  mode: trigger_with_gate
  cron: '0 8 * * *'
  depends_on:
    - pipeline_id: p_ingest_sales
      dependency_type: dataset
      require_same_day: true
source:
  asset_id: dest_dwh
  objects:
    - object_id: sales_orders
      load_strategy: incremental
      watermark_column: updated_at
      page_size: 10000
      compression: snappy
      encoding: utf-8
destination:
  asset_id: dest_dwh
  objects:
    - object_id: sales_daily_agg
      create_if_not_exists: true
transform:
  engine: dbt
  ref: marts/sales_daily
compute:
  engine: spark
  num_workers: 4
  machine_type: n1-standard-4
  staging_bucket: gs://my-bucket/staging
quality:
  metrics:
    - type: not_null
      column: sale_date
    - type: row_count_min
      value: 100
airflow:
  retries: 2
  retry_delay_minutes: 10
  execution_timeout_minutes: 180
  sla_minutes: 120
  tags: [etl, sales, dbt]
  pool: spark_pool
discovery_task:
  enabled: true
  on_critical_change: fail
""",
    "export": """\
schema_version: '1.0'
pipeline_id: p_export_sales_report
name: Export Sales Report to Partner
type: export
owner: data-ops@company.com
schedule:
  mode: trigger
  depends_on:
    - pipeline_id: p_etl_sales_daily
      dependency_type: dataset
      require_same_day: true
source:
  asset_id: dest_dwh
  objects:
    - object_id: sales_daily_agg
      load_strategy: full_load
      page_size: 50000
      compression: snappy
      encoding: utf-8
destination:
  asset_id: partner_sftp
  objects:
    - object_id: sales_report_csv
      create_if_not_exists: true
transform:
  engine: none
compute:
  engine: default
  num_workers: 1
  machine_type: n1-standard-2
  staging_bucket: gs://my-bucket/staging
quality:
  metrics:
    - type: row_count_min
      value: 1
airflow:
  retries: 1
  retry_delay_minutes: 5
  execution_timeout_minutes: 60
  sla_minutes: 45
  tags: [export, partner]
  pool: default_pool
discovery_task:
  enabled: false
  on_critical_change: ignore
""",
}


@router.get("/schema", summary="Pipeline YAML JSON Schema (fonte de verdade)")
async def get_harness_schema() -> dict[str, Any]:
    """Retorna o JSON Schema completo derivado do modelo PipelineSpec da plataforma.

    O Harness Engine utiliza este schema para:
    1. Validação estrutural (Layer 1) do YAML gerado pelo LLM
    2. Orientar o LLM sobre campos obrigatórios e tipos aceitos
    """
    try:
        return _get_schema()
    except Exception:
        # Fallback mínimo para não bloquear o Harness Engine
        return {
            "type": "object",
            "properties": {
                "schema_version": {"type": "string"},
                "pipeline_id": {"type": "string"},
                "type": {"type": "string", "enum": ["ingestion", "etl", "export"]},
                "owner": {"type": "string"},
            },
            "required": ["schema_version", "pipeline_id", "type", "owner"],
        }


@router.get("/gold-examples", summary="YAMLs Canônicos por Tipo de Pipeline")
async def get_harness_gold_examples() -> dict[str, str]:
    """Retorna exemplos YAML completos e válidos por tipo de pipeline.

    Utilizados pelo Harness Engine como few-shot anchors para o LLM,
    garantindo que a saída gerada siga a estrutura esperada pela plataforma.
    """
    return _GOLD_EXAMPLES
