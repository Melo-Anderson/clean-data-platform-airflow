from __future__ import annotations

import json
from pathlib import Path

from app.domain.objects.element_type import ElementType
from app.infrastructure.adapters.dataform.dataform_compilation_parser import (
    DataformCompilationParser,
    map_dataform_type_to_element_type,
)


def test_map_dataform_type_to_element_type() -> None:
    assert map_dataform_type_to_element_type("INT64") == ElementType.INTEGER
    assert map_dataform_type_to_element_type("BIGINT") == ElementType.BIGINT
    assert map_dataform_type_to_element_type("NUMERIC") == ElementType.DECIMAL
    assert map_dataform_type_to_element_type("FLOAT64") == ElementType.FLOAT
    assert map_dataform_type_to_element_type("TIMESTAMP") == ElementType.TIMESTAMP
    assert map_dataform_type_to_element_type("DATE") == ElementType.DATE
    assert map_dataform_type_to_element_type("BOOL") == ElementType.BOOLEAN
    assert map_dataform_type_to_element_type("BYTES") == ElementType.BYTES
    assert (
        map_dataform_type_to_element_type("RECORD") == ElementType.JSON
    )  # OBJECT não existe no enum
    assert map_dataform_type_to_element_type("STRUCT") == ElementType.JSON
    assert map_dataform_type_to_element_type("STRING") == ElementType.STRING
    assert map_dataform_type_to_element_type("UNKNOWN_TYPE") == ElementType.STRING


def test_parse_dict_extracts_tables_columns_and_dependencies() -> None:
    sample_compilation = {
        "tables": [
            {
                "target": {
                    "database": "gcp-project",
                    "schema": "platform_gold",
                    "name": "dim_players",
                },
                "type": "table",
                "actionDescriptor": {
                    "description": "Enriched players dimension",
                    "columns": [
                        {"path": ["player_sk"], "description": "Surrogate key"},
                        {"path": ["player_id"], "description": "Player natural key"},
                    ],
                },
                "dependencyTargets": [
                    {
                        "database": "gcp-project",
                        "schema": "platform_silver",
                        "name": "slv_players",
                    }
                ],
            }
        ],
        "declarations": [
            {
                "target": {
                    "database": "gcp-project",
                    "schema": "platform_bronze",
                    "name": "raw_gaming_events",
                },
                "actionDescriptor": {"description": "Raw gaming events source"},
            }
        ],
        "assertions": [
            {
                "target": {
                    "database": "gcp-project",
                    "schema": "platform_gold",
                    "name": "assert_unique_player_sk",
                },
                "dependencyTargets": [
                    {
                        "database": "gcp-project",
                        "schema": "platform_gold",
                        "name": "dim_players",
                    }
                ],
            }
        ],
    }

    parser = DataformCompilationParser()
    result = parser.parse_dict(sample_compilation)

    assert len(result.tables) == 1
    table = result.tables[0]
    assert table.name == "dim_players"
    assert table.schema == "platform_gold"
    assert table.type == "table"
    assert table.description == "Enriched players dimension"
    assert len(table.columns) == 2
    assert table.columns[0].name == "player_sk"
    assert table.dependency_targets == ["slv_players"]

    assert len(result.declarations) == 1
    decl = result.declarations[0]
    assert decl.name == "raw_gaming_events"
    assert decl.schema == "platform_bronze"

    assert len(result.assertions) == 1
    assert result.assertions[0].name == "assert_unique_player_sk"
    assert result.lineage == {"dim_players": ["slv_players"]}


def test_parse_file_reads_json_from_filesystem(tmp_path: Path) -> None:
    compilation_file = tmp_path / "compilation_result.json"
    compilation_file.write_text(
        json.dumps(
            {
                "tables": [
                    {
                        "target": {"schema": "silver", "name": "slv_players"},
                        "type": "incremental",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    parser = DataformCompilationParser()
    result = parser.parse_file(compilation_file)
    assert len(result.tables) == 1
    assert result.tables[0].name == "slv_players"
    assert result.tables[0].type == "incremental"
