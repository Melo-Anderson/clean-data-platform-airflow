from pathlib import Path

import sqlglot


def test_silver_models_exist_and_use_incremental_qualify_deduplication() -> None:
    silver_dir = Path("dbt_project/models/silver")
    expected_models = [
        ("slv_affiliate_cpa_ftd.sql", "player_id", "_ingested_at"),
        ("slv_players.sql", "player_id", "registration_timestamp"),
        ("slv_sessions.sql", "session_id", "session_start"),
        ("slv_transactions.sql", "transaction_id", "transaction_timestamp"),
    ]

    for model_file, pk, _ts_col in expected_models:
        file_path = silver_dir / model_file
        assert file_path.exists(), f"Missing silver model: {model_file}"

        content = file_path.read_text(encoding="utf-8")
        assert (
            "materialized='table'" in content
            or 'materialized="table"' in content
            or "materialized='incremental'" in content
            or 'materialized="incremental"' in content
        )
        assert "QUALIFY ROW_NUMBER() OVER" in content, (
            f"{model_file} does not use QUALIFY ROW_NUMBER()"
        )
        assert f"PARTITION BY {pk}" in content, (
            f"{model_file} does not partition by {pk} in QUALIFY"
        )
        assert "_processed_at" in content, f"{model_file} does not project _processed_at"

        # Replace Jinja tags for SQL syntax parsing validation
        clean_sql = content.replace("{{ ref('", "silver.").replace("') }}", "")
        clean_sql = clean_sql.replace("{{ config(", "/* config(").replace(") }}", ") */")
        clean_sql = clean_sql.replace("{% if is_incremental() %}", "/* is_incremental */")
        clean_sql = clean_sql.replace("{% endif %}", "/* endif */")
        clean_sql = clean_sql.replace("{{ this }}", "silver.this_table")
        parsed = sqlglot.parse_one(clean_sql, read="bigquery")
        assert parsed is not None
