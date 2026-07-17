# app/infrastructure/discovery/database_runner.py
from __future__ import annotations

import logging
from datetime import UTC, datetime

from sqlalchemy import Connection, inspect, text
from sqlalchemy.engine import Inspector
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.ext.asyncio import create_async_engine

from app.application.discovery.discovery_runner import DiscoveryRunner
from app.application.shared.secret_manager_port import SecretManagerPort
from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.endpoints.endpoint import DatabaseEndpoint, Endpoint
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
        scope_include: list[str],
        scope_exclude: list[str],
        endpoint: Endpoint,
    ) -> list[SchemaSnapshot]:
        """Connect once and reflect all requested tables in a single session."""
        if not isinstance(endpoint, DatabaseEndpoint):
            raise TypeError(
                f"DatabaseRunner only supports DatabaseEndpoint, got {type(endpoint).__name__}"
            )
        payload = await self._secret_manager.resolve(endpoint.credential_ref.path)
        url = build_connection_url(payload)

        engine = create_async_engine(url, pool_pre_ping=True)
        try:
            async with engine.connect() as conn:
                snapshots: list[SchemaSnapshot] = await conn.run_sync(
                    self._reflect_all_objects,
                    scope_include,
                    scope_exclude,
                )
        finally:
            await engine.dispose()

        return snapshots

    def _reflect_all_objects(
        self,
        sync_conn: Connection,
        scope_include: list[str],
        scope_exclude: list[str],
    ) -> list[SchemaSnapshot]:
        """
        Synchronous callback executed via conn.run_sync().
        Creates a single Inspector from the open connection and iterates all matching tables.
        """
        inspector = inspect(sync_conn)
        captured_at = datetime.now(UTC)

        table_targets = []
        print(
            f"!!! DatabaseRunner _reflect_all_objects called. scope_include={scope_include} !!!",
            flush=True,
        )
        for pattern in scope_include:
            if "." in pattern:
                schema, table = pattern.split(".", 1)
            else:
                schema = None
                table = pattern

            if table == "*":
                try:
                    names = inspector.get_table_names(schema=schema)
                    print(f"!!! Found tables in schema {schema}: {names} !!!", flush=True)
                except Exception as e:
                    print(f"!!! Failed to get table names for schema {schema}: {e} !!!", flush=True)
                    names = []
                for name in names:
                    full_name = f"{schema}.{name}" if schema else name
                    table_targets.append((name, schema, full_name))
            else:
                try:
                    names = inspector.get_table_names(schema=schema)
                    print(
                        f"!!! Found tables in schema {schema}: {names} (looking for {table}) !!!",
                        flush=True,
                    )
                except Exception as e:
                    print(f"!!! Failed to get table names for schema {schema}: {e} !!!", flush=True)
                    names = []
                if table in names:
                    full_name = f"{schema}.{table}" if schema else table
                    table_targets.append((table, schema, full_name))

        return [
            self._reflect_single_object(inspector, sync_conn, name, schema, full_name, captured_at)
            for name, schema, full_name in table_targets
        ]

    def _reflect_single_object(
        self,
        inspector: Inspector,
        sync_conn: Connection,
        table_name: str,
        schema: str | None,
        full_name: str,
        captured_at: datetime,
    ) -> SchemaSnapshot:
        """Reflect one table/view. Returns an empty SchemaSnapshot if the table does not exist."""
        try:
            columns = inspector.get_columns(table_name, schema=schema)
            pk_columns: set[str] = set(
                inspector.get_pk_constraint(table_name, schema=schema).get(
                    "constrained_columns", []
                )
            )
            fk_by_column: dict[str, str] = {
                col: fk["referred_table"]
                for fk in inspector.get_foreign_keys(table_name, schema=schema)
                for col in fk["constrained_columns"]
            }
            index_by_column: dict[str, list[str]] = {}
            for idx in inspector.get_indexes(table_name, schema=schema):
                idx_name = idx.get("name")
                if not idx_name:
                    continue
                for col in idx.get("column_names") or []:
                    if col:
                        index_by_column.setdefault(col, []).append(idx_name)

            try:
                table_comment: str | None = inspector.get_table_comment(
                    table_name, schema=schema
                ).get("text")
            except NotImplementedError:
                table_comment = None

            row_count = self._estimate_row_count(sync_conn, table_name, schema)

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
            logger.warning("Table %r not found; returning empty snapshot.", table_name)
            fields = []
            row_count = None

        return SchemaSnapshot(
            object_id="",  # Auto-provisioned objects don't have an ID until saved
            object_name=full_name,
            runner_type="database",
            captured_at=captured_at,
            row_count_estimate=row_count,
            fields=fields,
        )

    def _estimate_row_count(
        self, sync_conn: Connection, table_name: str, schema: str | None
    ) -> int | None:
        """COUNT(*) row count estimate. Returns None on any error."""
        try:
            full_ref = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
            result = sync_conn.execute(text(f"SELECT COUNT(*) FROM {full_ref}"))  # noqa: S608
            row = result.fetchone()
            return int(row[0]) if row else None
        except Exception:
            logger.debug("Could not count rows for %r", table_name, exc_info=True)
            return None
