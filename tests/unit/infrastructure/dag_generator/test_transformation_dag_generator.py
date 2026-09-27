import ast
from pathlib import Path

from app.infrastructure.dag_generator.dag_generator import (
    DagGenerator,
    _canonicalize_pipeline_dict,
)


def test_transformation_dag_generator_generates_valid_airflow3_asset_dag(tmp_path: Path) -> None:
    generator = DagGenerator()
    pipeline_dict = {
        "id": "pipe-transformation-001",
        "name": "Platform_Transformation_ETL",
        "type": "transformation",
        "owner": "analytics@co.com",
        "schedule": {
            "mode": "asset",
            "asset_uri": "platform://asset/raw_vault",
        },
        "source": {
            "asset_name": "raw_vault",
            "objects": [],
        },
        "destination": {
            "asset_name": "silver_vault",
            "objects": [{"object_name": "slv_orders"}, {"object_name": "dim_customers"}],
        },
        "compute": {
            "engine": "dbt",
            "staging_bucket": "/opt/airflow/logs/dbt_outputs",
            "config": {
                "project_dir": "/opt/airflow/dbt_project",
                "profiles_dir": "/opt/airflow/dbt_project",
            },
        },
        "quality": {
            "metrics": [{"name": "not_null"}],
        },
    }

    dag_code = generator.generate_transformation_dag(pipeline_dict)

    assert "Asset(" in dag_code
    assert "platform://asset/raw_vault" in dag_code
    assert "outlets=pipeline_outlets" in dag_code
    assert "platform://asset/silver_vault" in dag_code
    assert "dbt" in dag_code

    # Verify syntax validity
    compiled_ast = ast.parse(dag_code)
    assert compiled_ast is not None


def test_canonicalize_pipeline_dict_normalizes_dataform_staging_bucket() -> None:
    pipeline_dict = {
        "id": "pipe-dataform-001",
        "name": "Platform_Dataform_Transformation",
        "type": "transformation",
        "owner": "analytics@co.com",
        "compute": {
            "engine": "dataform",
            "select": "tag:silver",
        },
    }
    canonical = _canonicalize_pipeline_dict(pipeline_dict)
    assert canonical["compute"]["engine"] == "dataform"
    assert canonical["compute"]["staging_bucket"] == "/opt/airflow/logs/transformation_outputs"
    assert canonical["compute"]["select"] == "tag:silver"


def test_transformation_dag_generator_normalizes_dataform_without_dbt_staging_bucket() -> None:
    generator = DagGenerator()
    pipeline_dict = {
        "id": "pipe-dataform-001",
        "name": "Platform_Dataform_Transformation",
        "type": "transformation",
        "owner": "analytics@co.com",
        "compute": {
            "engine": "dataform",
            "select": "tag:silver",
        },
    }
    dag_code = generator.generate_transformation_dag(pipeline_dict)
    # Staging bucket NUNCA deve ser o default do dbt quando engine é outra
    assert "/opt/airflow/logs/dbt_outputs" not in dag_code


def test_transformation_dag_includes_params_with_pipeline_id() -> None:
    generator = DagGenerator()
    pipeline_dict = {
        "id": "pipe-transform-params-01",
        "name": "Platform_Params_Transformation",
        "type": "transformation",
        "owner": "analytics@co.com",
        "compute": {"engine": "dbt", "staging_bucket": "/tmp/test"},
    }
    dag_code = generator.generate_transformation_dag(pipeline_dict)
    assert "params=" in dag_code
    assert '"pipeline_id"' in dag_code
    assert "01_run_transformations" in dag_code
    assert "run_transformation_job(" in dag_code
    assert "evaluate_transformation_quality_gates(" in dag_code
    assert "sync_transformation_catalog_metadata(" in dag_code
