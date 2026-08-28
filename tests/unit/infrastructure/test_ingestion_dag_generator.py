from app.infrastructure.dag_generator.dag_generator import DagGenerator


def test_dag_generator_links_discovery_result_to_submit_compute_job():
    sample_yaml = """
pipeline:
  id: "pipeline-123"
  name: "Ingest_transactions"
  type: "ingestion"
  owner: "data-eng@company.com"
  schedule:
    mode: "cron"
    cron: "0 0 * * *"
  airflow:
    pool: "default_pool"
    schedule_interval: "@daily"
    catchup: false
    retries: 1
    retry_delay_seconds: 60
    sla_minutes: 60
    tags: ["ingestion"]
  source:
    asset: "otg_landing"
    objects:
      - object_id: "otg_landing.transactions"
  discovery_task:
    enabled: true
    on_critical_change: "block"
  destination:
    asset: "otg_bronze"
    objects:
      - object_name: "transactions"
  compute:
    engine: "omnibeam"
    staging_bucket: "gs://test-bucket"
    config:
      format: "csv"


"""
    generator = DagGenerator()
    dag_code = generator.generate(sample_yaml)

    assert (
        "def _submit_compute_job(discovery_result=None" in dag_code
        or "submit = _submit_compute_job(discovery_result=" in dag_code
    )
    assert "schema_snapshot" in dag_code
