# app/infrastructure/discovery/database_runner.py
from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import inspect, text
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.ext.asyncio import create_async_engine

from app.application.discovery.discovery_runner import DiscoveryRunner
from app.application.shared.secret_manager_port import SecretManagerPort
from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.endpoints.endpoint import DatabaseEndpoint
from app.domain.objects.data_object import DataObject
from app.infrastructure.discovery.connection_url_builder import build_connection_url
from app.infrastructure.discovery.sqlalchemy_type_mapper import map_sa_type_to_normalized

logger = logging.getLogger(__name__)


class DatabaseRunner(DiscoveryRunner):
    """
    DiscoveryRunner for relational databases using SQLAlchemy reflection.

    Performance contract:
      - ONE engine created per discovery invocation.
      - ONE connection opened for ALL tables in the asset.
      - ALL Inspector calls (columns, PKs, FKs, indexes, comments) happen inside
        a single conn.run_sync() to reuse the same DB cursor — minimising round trips.

    Metadata captured per table:
      - columns: name, source_type, normalized_type, nullable, column-level comment
      - primary key columns (is_primary_key=True on SchemaField)
      - foreign key references (SchemaField.extra["fk_to"] = referenced table name)
      - index membership (SchemaField.extra["indexes"] = list of index names)
      - table-level comment (SchemaField.extra["table_comment"])
      - row count estimate via COUNT(*) — returns None if permission denied
    """

    def __init__(self, secret_manager: SecretManagerPort) -> None:
        self._secret_manager = secret_manager

    async def run(
        self,
        asset_id: str,
        objects: list[DataObject],
        endpoint: DatabaseEndpoint,
    ) -> list[SchemaSnapshot]:
        """Connect once and reflect all requested DataObjects in a single session."""
        payload = await self._secret_manager.resolve(endpoint.credential_ref.path)
        url = build_connection_url(payload)

        engine = create_async_engine(url, pool_pre_ping=True)
        try:
            async with engine.connect() as conn:
                snapshots: list[SchemaSnapshot] = await conn.run_sync(
                    self._reflect_all_objects,
                    objects,
                )
        finally:
            await engine.dispose()

        return snapshots

    def _reflect_all_objects(
        self,
        sync_conn,
        objects: list[DataObject],
    ) -> list[SchemaSnapshot]:
        """
        Synchronous callback executed via conn.run_sync().
        Creates a single Inspector from the open connection and iterates all objects.
        """
        inspector = inspect(sync_conn)
        captured_at = datetime.now(timezone.utc)

        return [
            self._reflect_single_object(inspector, sync_conn, obj, captured_at)
            for obj in objects
        ]

    def _reflect_single_object(
        self,
        inspector,
        sync_conn,
        obj: DataObject,
        captured_at: datetime,
    ) -> SchemaSnapshot:
        """Reflect one table/view. Returns an empty SchemaSnapshot if the table does not exist."""
        try:
            columns = inspector.get_columns(obj.name)
            pk_columns: set[str] = set(
                inspector.get_pk_constraint(obj.name).get("constrained_columns", [])
            )
            fk_by_column: dict[str, str] = {
                col: fk["referred_table"]
                for fk in inspector.get_foreign_keys(obj.name)
                for col in fk["constrained_columns"]
            }
            index_by_column: dict[str, list[str]] = {}
            for idx in inspector.get_indexes(obj.name):
                for col in idx.get("column_names") or []:
                    index_by_column.setdefault(col, []).append(idx["name"])

            try:
                table_comment: str | None = inspector.get_table_comment(obj.name).get("text")
            except NotImplementedError:
                table_comment = None

            row_count = self._estimate_row_count(sync_conn, obj.name)

            fields = [
                SchemaField(
                    name=col["name"],
                    source_type=str(col["type"]),
                    normalized_type=map_sa_type_to_normalized(col["type"]),
                    nullable=bool(col.get("nullable", True)),
                    is_primary_key=col["name"] in pk_columns,
                    description=col.get("comment"),
                    extra={
                        "fk_to": fk_by_column.get(col["name"]),
                        "indexes": index_by_column.get(col["name"], []),
                        "table_comment": table_comment,
                    },
                )
                for col in columns
            ]

        except NoSuchTableError:
            logger.warning("Table %r not found; returning empty snapshot.", obj.name)
            fields = []
            row_count = None

        return SchemaSnapshot(
            object_id=obj.id,
            object_name=obj.name,
            runner_type="database",
            captured_at=captured_at,
            row_count_estimate=row_count,
            fields=fields,
        )

    def _estimate_row_count(self, sync_conn, table_name: str) -> int | None:
        """COUNT(*) row count estimate. Returns None on any error."""
        try:
            result = sync_conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))  # noqa: S608
            row = result.fetchone()
            return int(row[0]) if row else None
        except Exception:
            logger.debug("Could not count rows for %r", table_name, exc_info=True)
            return None
