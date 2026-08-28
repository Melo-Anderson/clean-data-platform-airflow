import json
from pathlib import Path

from app.infrastructure.adapters.compute.omnibeam_compute_adapter import _build_manifest_for_job
from app.infrastructure.adapters.omnibeam.omnibeam_manifest_builder import OmniBeamManifestBuilder


def test_omnibeam_manifest_builder_builds_fields_from_dict_snapshot():
    builder = OmniBeamManifestBuilder()
    snapshot = {
        "fields": [
            {"name": "transaction_id", "type": "string", "nullable": False},
            {"name": "player_id", "type": "string", "nullable": False},
            {"name": "amount", "type": "decimal", "nullable": True},
            {"name": "created_at", "type": "timestamp", "nullable": True},
        ]
    }
    schema_wrapper = builder.build_fields_schema(snapshot)
    field_names = [f.name for f in schema_wrapper.fields]
    assert field_names == ["transaction_id", "player_id", "amount", "created_at"]
    assert schema_wrapper.fields[0].type == "string"
    assert schema_wrapper.fields[2].type == "decimal"
    assert schema_wrapper.fields[2].scale == 2


def test_build_manifest_for_job_injects_snapshot_fields_into_manifest(tmp_path: Path):
    config = {
        "format": "csv",
        "source_objects": [{"object_id": "asset.transactions"}],
        "source_type": "storage",
        "schema_snapshot": {
            "fields": [
                {"name": "transaction_id", "type": "string", "nullable": False},
                {"name": "player_id", "type": "string", "nullable": False},
                {"name": "amount", "type": "decimal", "nullable": True},
            ]
        },
        "quality_rules": [{"type": "not_null", "column": "transaction_id"}],
    }
    manifest_json = _build_manifest_for_job(
        pipeline_id="p-123",
        job_id="job-999",
        config=config,
        output_dir=tmp_path,
    )
    manifest = json.loads(manifest_json)
    fields = manifest["source"]["schema"]["fields"]
    assert len(fields) == 3
    assert fields[0]["name"] == "transaction_id"
    assert fields[1]["name"] == "player_id"
    assert fields[2]["name"] == "amount"
    assert manifest["quality_config"]["rules"] == [{"type": "not_null", "column": "transaction_id"}]
