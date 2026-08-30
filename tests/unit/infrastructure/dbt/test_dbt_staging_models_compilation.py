from pathlib import Path

import pytest
import sqlglot

DBT_PROJECT_DIR = Path("dbt_project")
pytestmark = pytest.mark.skipif(
    not DBT_PROJECT_DIR.exists() or not (DBT_PROJECT_DIR / "dbt_project.yml").exists(),
    reason="dbt_project is gitignored and not present in CI checkout environment",
)


def test_staging_models_exist_and_parse_bigquery_syntax() -> None:
    staging_dir = Path("dbt_project/models/staging")
    expected_models = [
        "stg_affiliate_cpa_ftd.sql",
        "stg_players.sql",
        "stg_sessions.sql",
        "stg_transactions.sql",
    ]

    for model_file in expected_models:
        file_path = staging_dir / model_file
        assert file_path.exists(), f"Missing staging model: {model_file}"

        content = file_path.read_text(encoding="utf-8")
        assert "source('platform_bronze'" in content, (
            f"{model_file} does not select from platform_bronze source"
        )
        assert "_ingested_at" in content, f"{model_file} does not project _ingested_at"
        assert "_transformed_at" in content, f"{model_file} does not project _transformed_at"

        # Replace Jinja tags for SQL syntax parsing validation
        clean_sql = content.replace("{{ source('platform_bronze', '", "platform_bronze.").replace(
            "') }}", ""
        )
        clean_sql = clean_sql.replace("{{ config(", "/* config(").replace(") }}", ") */")
        parsed = sqlglot.parse_one(clean_sql, read="bigquery")
        assert parsed is not None
