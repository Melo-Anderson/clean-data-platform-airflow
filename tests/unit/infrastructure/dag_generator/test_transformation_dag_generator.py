import ast
from pathlib import Path

from app.infrastructure.dag_generator.dag_generator import DagGenerator


def test_transformation_dag_generator_generates_valid_airflow3_asset_dag(tmp_path: Path) -> None:
    generator = DagGenerator()
    pipeline_dict = {
        "id": "pipe-transformation-001",
        "name": "Platform_Transformation_ETL",
        "pipeline_type": "transformation",
        "owner": "analytics@co.com",
        "schedule": {
            "mode": "asset",
            "cron_schedule": "",
            "asset_uri": "platform://asset/raw_vault",
        },
        "source_asset": "raw_vault",
        "destination_assets": ["silver_vault", "gold_analytics"],
        "source_objects": [],
        "destination_objects": [{"object_name": "slv_orders"}, {"object_name": "dim_customers"}],
        "compute": {
            "engine": "dbt",
            "project_dir": "/opt/airflow/dbt_project",
            "profiles_dir": "/opt/airflow/dbt_project",
            "staging_bucket": "/opt/airflow/logs/dbt_outputs",
        },
        "quality_rules": [{"type": "not_null"}],
    }

    dag_code = generator.generate_transformation_dag(pipeline_dict)

    assert "Asset(" in dag_code
    assert "platform://asset/raw_vault" in dag_code
    assert "outlets=pipeline_outlets" in dag_code
    assert "platform://asset/silver_vault" in dag_code
    assert "platform://asset/gold_analytics" in dag_code
    assert "dbt" in dag_code

    # Verify syntax validity
    compiled_ast = ast.parse(dag_code)
    assert compiled_ast is not None
