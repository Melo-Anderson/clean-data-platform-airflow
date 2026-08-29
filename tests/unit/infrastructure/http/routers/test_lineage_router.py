from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from app.infrastructure.persistence.database import get_session_factory
from app.infrastructure.persistence.models.lineage_mapping_model import LineageMappingModel


@pytest.mark.asyncio
async def test_emit_raw_lineage_persists_lineage_mappings_with_schema(
    client: AsyncClient, tmp_path: Path
) -> None:
    schema_file = tmp_path / "schema.json"
    schema_file.write_text(
        json.dumps(
            {
                "fields": [
                    {"name": "player_id", "type": "STRING"},
                    {"name": "email", "type": "STRING"},
                ]
            }
        ),
        encoding="utf-8",
    )

    pipe_id = f"pipe-{uuid.uuid4().hex[:8]}"
    source_obj = f"asset-platform.{pipe_id}.src"
    dest_obj = f"{pipe_id}_table"

    res = await client.post(
        "/v1/lineage/raw",
        json={
            "pipeline_id": pipe_id,
            "source_object_ids": [source_obj],
            "destination_object_ids": [dest_obj],
            "schema_path": str(schema_file),
        },
    )
    assert res.status_code == 200
    assert res.json()["status"] == "ok"

    session_factory = get_session_factory()
    async with session_factory() as session:
        query = select(LineageMappingModel).filter_by(
            pipeline_id=pipe_id,
            source_object_id=source_obj,
            destination_object_id=dest_obj,
        )
        row = (await session.execute(query)).scalar_one_or_none()
        assert row is not None
        assert row.column_mappings == [
            {"source_column": "player_id", "destination_column": "player_id", "expression": ""},
            {"source_column": "email", "destination_column": "email", "expression": ""},
        ]


@pytest.mark.asyncio
async def test_emit_raw_lineage_persists_direct_copy_without_schema(
    client: AsyncClient,
) -> None:
    pipe_id = f"pipe-{uuid.uuid4().hex[:8]}"
    source_obj = f"asset-platform.{pipe_id}.src"
    dest_obj = f"{pipe_id}_table"

    res = await client.post(
        "/v1/lineage/raw",
        json={
            "pipeline_id": pipe_id,
            "source_object_ids": [source_obj],
            "destination_object_ids": [dest_obj],
            "schema_path": None,
        },
    )
    assert res.status_code == 200

    session_factory = get_session_factory()
    async with session_factory() as session:
        query = select(LineageMappingModel).filter_by(
            pipeline_id=pipe_id,
            source_object_id=source_obj,
            destination_object_id=dest_obj,
        )
        row = (await session.execute(query)).scalar_one_or_none()
        assert row is not None
        assert row.column_mappings == [
            {"source_column": "*", "destination_column": "*", "expression": "DIRECT_COPY"}
        ]
