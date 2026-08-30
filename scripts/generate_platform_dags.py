from __future__ import annotations

import contextlib
from pathlib import Path

from app.infrastructure.dag_generator.dag_generator import DagGenerator


def main() -> None:
    gen = DagGenerator()
    dags_dir = Path("dags")
    dags_dir.mkdir(exist_ok=True)

    # 1. Silver Transformation DAG
    silver_pipe = {
        "id": "pipe-platform-silver-001",
        "name": "Platform_Silver_ETL",
        "pipeline_type": "transformation",
        "owner": "analytics@company.com",
        "schedule": {
            "mode": "asset",
            "cron_schedule": "",
            "asset_uri": "platform://asset/platform_bronze",
        },
        "source_asset": "platform_bronze",
        "destination_assets": ["platform_silver"],
        "source_objects": [],
        "destination_objects": [
            {"object_name": "slv_players"},
            {"object_name": "slv_sessions"},
            {"object_name": "slv_transactions"},
            {"object_name": "slv_affiliate_cpa_ftd"},
        ],
        "compute": {
            "engine": "dbt",
            "select": "staging silver",
            "project_dir": "/opt/airflow/dbt_project",
            "profiles_dir": "/opt/airflow/dbt_project",
            "staging_bucket": "/opt/airflow/logs/dbt_outputs",
        },
        "quality_rules": [{"type": "not_null"}],
    }
    (dags_dir / "dag_p_Platform_Silver_ETL.py").write_text(
        gen.generate_transformation_dag(silver_pipe), encoding="utf-8"
    )

    # 2. Gold Transformation DAG
    gold_pipe = {
        "id": "pipe-platform-gold-001",
        "name": "Platform_Gold_Analytics",
        "pipeline_type": "transformation",
        "owner": "analytics@company.com",
        "schedule": {
            "mode": "asset",
            "cron_schedule": "",
            "asset_uri": "platform://asset/platform_silver",
        },
        "source_asset": "platform_silver",
        "destination_assets": ["platform_gold"],
        "source_objects": [],
        "destination_objects": [
            {"object_name": "dim_players"},
            {"object_name": "dim_affiliates"},
            {"object_name": "fct_transactions"},
            {"object_name": "fct_affiliate_performance"},
            {"object_name": "fct_player_risk_profile"},
            {"object_name": "gold_fraud_alerts"},
        ],
        "compute": {
            "engine": "dbt",
            "select": "gold",
            "project_dir": "/opt/airflow/dbt_project",
            "profiles_dir": "/opt/airflow/dbt_project",
            "staging_bucket": "/opt/airflow/logs/dbt_outputs",
        },
        "quality_rules": [{"type": "not_null"}],
    }
    (dags_dir / "dag_p_Platform_Gold_Analytics.py").write_text(
        gen.generate_transformation_dag(gold_pipe), encoding="utf-8"
    )

    # 3. Ingestion DAGs
    sources = [
        ("players", "json"),
        ("sessions", "json"),
        ("transactions", "csv"),
        ("affiliate_cpa_ftd", "csv"),
    ]
    for obj_name, fmt in sources:
        yaml_content = f"""pipeline:
  id: "pipe-ingest-{obj_name}"
  name: "Ingest_{obj_name}_Platform_Bronze"
  type: "ingestion"
  owner: "data-engineering@company.com"
  schedule:
    mode: "cron"
    cron: "0 * * * *"
  airflow:
    pool: "default_pool"
    schedule_interval: "@hourly"
    catchup: false
    retries: 1
    retry_delay_seconds: 60
    sla_minutes: 90
    tags: ["ingestion", "bronze", "platform"]
  source:
    asset: "platform_landing"
    objects:
      - object_id: "asset-platform-bronze.{obj_name}"
  discovery_task:
    enabled: true
    on_critical_change: "block"
  destination:
    asset: "platform_bronze"
    objects:
      - object_name: "{obj_name}"
  compute:
    engine: "omnibeam"
    staging_bucket: "logs/omnibeam_outputs"
    config:
      format: "{fmt}"
"""
        code = gen.generate(yaml_content)
        (dags_dir / f"dag_p_Ingest_{obj_name}_Platform_Bronze.py").write_text(
            code, encoding="utf-8"
        )

    # 4. Remove old DAGs with OTG in their names
    for old_dag in dags_dir.glob("*OTG*"):
        with contextlib.suppress(Exception):
            old_dag.unlink()

    for old_dag in dags_dir.glob("Ingest_*_OTG_Bronze.py"):
        with contextlib.suppress(Exception):
            old_dag.unlink()

    print("Successfully generated all Platform DAGs and cleaned up old DAG files!")


if __name__ == "__main__":
    main()
