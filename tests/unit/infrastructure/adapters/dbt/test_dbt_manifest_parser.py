import json
from pathlib import Path

from app.infrastructure.adapters.dbt.dbt_manifest_parser import DbtManifestParser


def test_dbt_manifest_parser_extracts_models_and_lineage(tmp_path: Path) -> None:
    fake_manifest = {
        "nodes": {
            "model.platform.stg_players": {
                "unique_id": "model.platform.stg_players",
                "name": "stg_players",
                "resource_type": "model",
                "schema": "platform_silver",
                "description": "Staging players",
                "depends_on": {"nodes": ["source.platform.platform_bronze.players"]},
                "columns": {
                    "player_id": {"name": "player_id", "data_type": "STRING", "description": "ID"},
                    "email": {"name": "email", "data_type": "STRING", "description": "Email"},
                },
            },
            "model.platform.slv_players": {
                "unique_id": "model.platform.slv_players",
                "name": "slv_players",
                "resource_type": "model",
                "schema": "platform_silver",
                "description": "Silver players",
                "depends_on": {"nodes": ["model.platform.stg_players"]},
                "columns": {
                    "player_id": {"name": "player_id", "data_type": "STRING", "description": "ID"},
                    "email": {"name": "email", "data_type": "STRING", "description": "Email"},
                },
            },
        },
        "sources": {
            "source.platform.platform_bronze.players": {
                "unique_id": "source.platform.platform_bronze.players",
                "name": "players",
                "source_name": "platform_bronze",
                "resource_type": "source",
                "schema": "platform_bronze",
                "description": "Bronze players",
                "columns": {
                    "player_id": {"name": "player_id", "data_type": "STRING", "description": "ID"}
                },
            }
        },
    }

    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text(json.dumps(fake_manifest), encoding="utf-8")

    parser = DbtManifestParser()
    parsed = parser.parse_file(manifest_file)

    assert len(parsed.models) == 2
    assert len(parsed.sources) == 1
    assert "model.platform.slv_players" in parsed.lineage
    assert parsed.lineage["model.platform.slv_players"] == ["model.platform.stg_players"]
