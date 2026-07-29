"""SQLAlchemy read-only adapter implementing MetadataPort."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


@dataclass
class ColumnMeta:
    name: str
    data_type: str
    is_primary_key: bool
    is_indexed: bool
    has_description: bool
    policy_tags: list[str] = field(default_factory=list)


@dataclass
class ObjectMeta:
    object_id: str
    asset_id: str
    object_type: str
    columns: list[ColumnMeta] = field(default_factory=list)
    required_stewards: list[str] = field(default_factory=list)
    owner_email: str = ""


class DbSchemaReader:
    def __init__(self, db_url: str) -> None:
        self._engine: Engine = create_engine(db_url, pool_pre_ping=True)

    def get_object_metadata(self, asset_id: str, object_id: str) -> ObjectMeta | None:
        with self._engine.connect() as conn:
            row = conn.execute(
                text("SELECT id, type FROM data_objects WHERE asset_id = :a AND name = :n"),
                {"a": asset_id, "n": object_id},
            ).fetchone()
            if row is None:
                return None
            return ObjectMeta(
                object_id=object_id,
                asset_id=asset_id,
                object_type=str(row[1]),
                columns=self._cols(conn, str(row[0])),
            )

    def list_objects_for_asset(self, asset_id: str) -> list[ObjectMeta]:
        with self._engine.connect() as conn:
            rows = conn.execute(
                text("SELECT id, name, type FROM data_objects WHERE asset_id = :a"), {"a": asset_id}
            ).fetchall()
            return [
                ObjectMeta(
                    object_id=str(r[1]),
                    asset_id=asset_id,
                    object_type=str(r[2]),
                    columns=self._cols(conn, str(r[0])),
                )
                for r in rows
            ]

    def _cols(self, conn: Any, obj_id: str) -> list[ColumnMeta]:
        rows = conn.execute(
            text(
                "SELECT name, data_type, is_primary_key, is_indexed, description "
                "FROM data_elements WHERE object_id = :o"
            ),
            {"o": obj_id},
        ).fetchall()
        return [ColumnMeta(str(r[0]), str(r[1]), bool(r[2]), bool(r[3]), bool(r[4])) for r in rows]
