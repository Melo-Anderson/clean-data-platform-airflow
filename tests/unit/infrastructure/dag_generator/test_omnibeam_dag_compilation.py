from __future__ import annotations

from app.infrastructure.dag_generator.dag_generator import DagGenerator


def test_compile_ingestion_dag_dataflow_flex_template() -> None:
    generator = DagGenerator()
    pipeline_yaml = """
id: pipe-omnibeam-dataflow
name: omnibeam_orders_ingest
type: ingestion
owner: data@co.com
schedule:
  mode: cron
  cron: "0 0 * * *"
airflow:
  pool: default_pool
  sla_minutes: 60
  tags: [omnibeam, gcp]
  retries: 1
  retry_delay_minutes: 5
  execution_timeout_minutes: 60
discovery_task:
  enabled: true
  on_critical_change: block
compute:
  engine: dataflow
  staging_bucket: gs://landing-bucket/staging
  config:
    num_workers: 2
    machine_type: n1-standard-2
source:
  asset: asset-landing-files
  objects:
    - name: orders
      object_id: orders
destination:
  asset: asset-lakehouse
  objects:
    - object_name: orders
quality:
  metrics:
    - type: not_null
      column: id
"""
    code = generator.generate(pipeline_yaml)
    assert "DataflowStartFlexTemplateOperator" in code
    assert "omnibeam_orders_ingest" in code
    assert "deferrable=True" in code


def test_compile_ingestion_dag_omnibeam_local() -> None:
    generator = DagGenerator()
    pipeline_yaml = """
id: pipe-omnibeam-local
name: omnibeam_local_ingest
type: ingestion
owner: data@co.com
schedule:
  mode: cron
  cron: "0 0 * * *"
airflow:
  pool: default_pool
  sla_minutes: 30
  tags: [omnibeam, local]
  retries: 1
  retry_delay_minutes: 5
  execution_timeout_minutes: 60
discovery_task:
  enabled: true
  on_critical_change: block
compute:
  engine: omnibeam
  staging_bucket: /tmp/staging
  config:
    num_workers: 1
    machine_type: local
source:
  asset: asset-local-files
  objects:
    - name: local_orders
      object_id: local_orders
destination:
  asset: asset-lakehouse
  objects:
    - object_name: orders
quality:
  metrics: []
"""
    code = generator.generate(pipeline_yaml)
    assert "submit_compute_job" in code
    assert "omnibeam_local_ingest" in code
