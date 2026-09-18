from __future__ import annotations

from pathlib import Path

import pytest

from app.infrastructure.adapters.dataform.dataform_compilation_parser import (
    DataformCompilationParser,
)


@pytest.mark.skipif(
    not Path("dataform_project/workflow_settings.yaml").exists(),
    reason="dataform_project is gitignored and not present in CI checkout environment",
)
def test_dataform_project_structure_exists() -> None:
    project_dir = Path("dataform_project")
    assert project_dir.is_dir()
    assert (project_dir / "workflow_settings.yaml").is_file()
    assert (project_dir / "dataform.json").is_file()
    assert (project_dir / "package.json").is_file()
    assert (project_dir / "includes" / "helpers.js").is_file()
    assert (project_dir / "definitions" / "sources" / "declarations.js").is_file()

    # Staging sqlx
    assert (project_dir / "definitions" / "staging" / "stg_players.sqlx").is_file()
    assert (project_dir / "definitions" / "staging" / "stg_transactions.sqlx").is_file()
    assert (project_dir / "definitions" / "staging" / "stg_sessions.sqlx").is_file()
    assert (project_dir / "definitions" / "staging" / "stg_affiliate_cpa_ftd.sqlx").is_file()

    # Silver sqlx
    assert (project_dir / "definitions" / "silver" / "slv_players.sqlx").is_file()
    assert (project_dir / "definitions" / "silver" / "slv_transactions.sqlx").is_file()
    assert (project_dir / "definitions" / "silver" / "slv_sessions.sqlx").is_file()
    assert (project_dir / "definitions" / "silver" / "slv_affiliate_cpa_ftd.sqlx").is_file()

    # Gold sqlx
    assert (project_dir / "definitions" / "gold" / "dimensions" / "dim_players.sqlx").is_file()
    assert (project_dir / "definitions" / "gold" / "dimensions" / "dim_affiliates.sqlx").is_file()
    assert (project_dir / "definitions" / "gold" / "facts" / "fct_transactions.sqlx").is_file()
    assert (
        project_dir / "definitions" / "gold" / "facts" / "fct_player_risk_profile.sqlx"
    ).is_file()
    assert (
        project_dir / "definitions" / "gold" / "facts" / "fct_affiliate_performance.sqlx"
    ).is_file()
    assert (project_dir / "definitions" / "gold" / "fraud" / "gold_fraud_alerts.sqlx").is_file()

    # Assertions
    assert (project_dir / "definitions" / "assertions" / "assert_unique_player_id.sqlx").is_file()
    assert (
        project_dir / "definitions" / "assertions" / "assert_valid_transaction_amounts.sqlx"
    ).is_file()
    assert (project_dir / "definitions" / "assertions" / "assert_non_negative_cpa.sqlx").is_file()


@pytest.mark.skipif(
    not Path("dataform_project/compilation_result.json").exists(),
    reason="dataform_project is gitignored and not present in CI checkout environment",
)
def test_dataform_project_compilation_parses_successfully() -> None:
    compilation_path = Path("dataform_project/compilation_result.json")
    assert compilation_path.is_file()

    parser = DataformCompilationParser()
    result = parser.parse_file(compilation_path)

    # 4 Declarations (Bronze sources)
    assert len(result.declarations) == 4
    decl_names = {d.name for d in result.declarations}
    assert decl_names == {
        "raw_players",
        "raw_transactions",
        "raw_sessions",
        "raw_affiliate_cpa_ftd",
    }
    for decl in result.declarations:
        assert decl.schema == "platform_bronze"
        assert len(decl.columns) > 0

    # 10 Tables/Views (4 staging, 4 silver, 2 dim, 3 fct, 1 fraud = 14 total tables)
    assert len(result.tables) == 14
    table_names = {t.name for t in result.tables}
    expected_tables = {
        "stg_players",
        "stg_transactions",
        "stg_sessions",
        "stg_affiliate_cpa_ftd",
        "slv_players",
        "slv_transactions",
        "slv_sessions",
        "slv_affiliate_cpa_ftd",
        "dim_players",
        "dim_affiliates",
        "fct_transactions",
        "fct_player_risk_profile",
        "fct_affiliate_performance",
        "gold_fraud_alerts",
    }
    assert table_names == expected_tables

    # 3 Assertions
    assert len(result.assertions) == 3
    assertion_names = {a.name for a in result.assertions}
    assert assertion_names == {
        "assert_unique_player_id",
        "assert_valid_transaction_amounts",
        "assert_non_negative_cpa",
    }

    # Verify Lineage
    assert result.lineage["stg_players"] == ["raw_players"]
    assert result.lineage["slv_players"] == ["stg_players"]
    assert "slv_players" in result.lineage["dim_players"]
    assert "dim_players" in result.lineage["fct_transactions"]
    assert "fct_player_risk_profile" in result.lineage["gold_fraud_alerts"]
