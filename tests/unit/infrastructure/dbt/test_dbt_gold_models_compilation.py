import re
from pathlib import Path

import sqlglot


def _clean_jinja(sql: str) -> str:
    # Replace surrogate key macros with dummy string
    sql = re.sub(
        r"\{\{\s*dbt_utils\.generate_surrogate_key\(.*?\)\s*\}\}",
        "'dummy_surrogate_key'",
        sql,
        flags=re.DOTALL,
    )
    # Replace var() with defaults
    sql = re.sub(r"\{\{\s*var\('[^']+',\s*([^)]+)\)\s*\}\}", r"\1", sql)
    # Replace ref() with table name
    sql = re.sub(r"\{\{\s*ref\('([^']+)'\)\s*\}\}", r"silver.\1", sql)
    # Replace {{ this }}
    sql = re.sub(r"\{\{\s*this\s*\}\}", "gold.this_table", sql)
    # Replace config blocks with comments
    sql = re.sub(r"\{\{\s*config\(.*?\)\s*\}\}", "/* config */", sql, flags=re.DOTALL)
    # Replace {% if ... %} and {% endif %} blocks
    sql = re.sub(r"\{%.*?%\}", "/* jinja */", sql)
    return sql


def test_gold_models_exist_and_parse_bigquery_syntax() -> None:
    gold_dir = Path("dbt_project/models/gold")
    expected_models = [
        ("dimensions/dim_players.sql", "dim_players"),
        ("dimensions/dim_affiliates.sql", "dim_affiliates"),
        ("facts/fct_transactions.sql", "fct_transactions"),
        ("facts/fct_affiliate_performance.sql", "fct_affiliate_performance"),
        ("facts/fct_player_risk_profile.sql", "fct_player_risk_profile"),
        ("fraud/gold_fraud_alerts.sql", "gold_fraud_alerts"),
    ]

    for rel_path, model_name in expected_models:
        file_path = gold_dir / rel_path
        assert file_path.exists(), f"Missing gold model: {rel_path}"

        content = file_path.read_text(encoding="utf-8")
        assert "_calculated_at" in content or "_transformed_at" in content

        clean_sql = _clean_jinja(content)
        parsed = sqlglot.parse_one(clean_sql, read="bigquery")
        assert parsed is not None, f"Failed to parse BigQuery SQL for {model_name}"
