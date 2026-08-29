import re
from pathlib import Path

import sqlglot


def test_custom_singular_tests_exist_and_parse() -> None:
    tests_dir = Path("dbt_project/tests")
    expected_tests = [
        "assert_positive_deposit_amount.sql",
        "assert_cpa_qualified_has_valid_ftd.sql",
        "assert_net_ggr_matches_wagers_and_wins.sql",
    ]

    for test_file in expected_tests:
        file_path = tests_dir / test_file
        assert file_path.exists(), f"Missing singular test file: {test_file}"

        content = file_path.read_text(encoding="utf-8")
        clean_sql = re.sub(r"\{\{\s*ref\('([^']+)'\)\s*\}\}", r"silver.\1", content)
        clean_sql = re.sub(
            r"\{\{\s*config\(.*?\)\s*\}\}", "/* config */", clean_sql, flags=re.DOTALL
        )
        parsed = sqlglot.parse_one(clean_sql, read="bigquery")
        assert parsed is not None, f"Failed to parse {test_file}"


def test_doc_blocks_exist() -> None:
    docs_file = Path("dbt_project/models/gold/gold_docs.md")
    assert docs_file.exists(), "Missing gold_docs.md"
    content = docs_file.read_text(encoding="utf-8")
    assert "{% docs player_risk_tier %}" in content
    assert "{% docs cpa_commission %}" in content
    assert "{% docs fraud_alert_type %}" in content
