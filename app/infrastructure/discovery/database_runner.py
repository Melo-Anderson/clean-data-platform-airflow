# app/infrastructure/discovery/database_runner.py
from __future__ import annotations

import fnmatch
import logging
from datetime import UTC, datetime

from sqlalchemy import Connection, inspect
from sqlalchemy.engine import Inspector
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.ext.asyncio import create_async_engine

from app.application.discovery.discovery_runner import DiscoveryRunner
from app.application.shared.secret_manager_port import SecretManagerPort
from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.endpoints.endpoint import DatabaseEndpoint, Endpoint
from app.infrastructure.discovery.connection_url_builder import build_connection_url
from app.infrastructure.discovery.profiler_strategy import (
    DatabaseProfilerStrategy,
    get_profiler_strategy,
)
from app.infrastructure.discovery.sqlalchemy_type_mapper import map_sa_type_to_normalized

logger = logging.getLogger(__name__)


class DatabaseRunner(DiscoveryRunner):
    """DiscoveryRunner for relational databases using SQLAlchemy reflection.

    Performance contract:
      - ONE engine created per discovery invocation.
      - ONE connection opened for ALL tables in the asset.
      - ONE get_table_names() call — glob filtering is performed in-memory via fnmatch.
      - Strategy pattern for database-specific profiling (e.g. Postgres pg_class stats).
      - ALL Inspector calls happen inside a single conn.run_sync().
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
        schema = payload.get("schema")

        engine = create_async_engine(url, pool_pre_ping=True)
        try:
            async with engine.connect() as conn:
                snapshots: list[SchemaSnapshot] = await conn.run_sync(
                    self._reflect_all_objects,
                    scope_include,
                    scope_exclude,
                    schema,
                )
        finally:
            await engine.dispose()

        return snapshots

    def _reflect_all_objects(
        self,
        sync_conn: Connection,
        scope_include: list[str],
        scope_exclude: list[str],
        schema: str | None = None,
    ) -> list[SchemaSnapshot]:
        """Synchronous callback executed via conn.run_sync().

        Creates a single Inspector from the open connection, fetches table names ONCE,
        applies fnmatch filtering for scope_include and scope_exclude, and reflects all targets.
        """
        inspector = inspect(sync_conn)
        captured_at = datetime.now(UTC)

        profiler = get_profiler_strategy(sync_conn.dialect.name)

        # 1. Fetch table names ONCE
        try:
            all_table_names = inspector.get_table_names(schema=schema)
            if not all_table_names:
                all_table_names = inspector.get_table_names(schema=None)
        except Exception as e:
            logger.warning("Failed to list tables for schema %r: %s", schema, e)
            all_table_names = []

        # 2. Filter via scope_include (fnmatch glob matching)
        included_names: set[str] = set()
        for pattern in scope_include:
            if pattern == "*":
                included_names.update(all_table_names)
            else:
                for name in all_table_names:
                    fname = f"{schema}.{name}" if schema else f"public.{name}"
                    if fnmatch.fnmatch(name, pattern) or fnmatch.fnmatch(fname, pattern):
                        included_names.add(name)

        # 3. Filter out via scope_exclude (fnmatch glob matching)
        final_names: list[str] = []
        for name in sorted(included_names):
            excluded = False
            fname = f"{schema}.{name}" if schema else f"public.{name}"
            for ex_pattern in scope_exclude:
                if fnmatch.fnmatch(name, ex_pattern) or fnmatch.fnmatch(fname, ex_pattern):
                    excluded = True
                    break
            if not excluded:
                final_names.append(name)

        # 4. Reflect each target table
        return [
            self._reflect_single_object(
                inspector=inspector,
                sync_conn=sync_conn,
                profiler=profiler,
                table_name=name,
                schema=schema,
                full_name=f"{schema}.{name}" if schema else f"public.{name}",
                captured_at=captured_at,
            )
            for name in final_names
        ]

    def _reflect_single_object(
        self,
        inspector: Inspector,
        sync_conn: Connection,
        profiler: DatabaseProfilerStrategy,
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
            raw_indexes = inspector.get_indexes(table_name, schema=schema)
            raw_fks = inspector.get_foreign_keys(table_name, schema=schema)

            fk_by_column: dict[str, str] = {
                col: fk["referred_table"]
                for fk in raw_fks
                for col in fk.get("constrained_columns", [])
            }
            index_by_column: dict[str, list[str]] = {}
            for idx in raw_indexes:
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

            row_count = profiler.estimate_row_count(sync_conn, table_name, schema)

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

            snapshot_extra = {
                "schema": schema,
                "full_name": full_name,
                "indexes": [
                    {
                        "name": idx.get("name") or "unnamed_idx",
                        "columns": [c for c in (idx.get("column_names") or []) if c],
                        "unique": bool(idx.get("unique", False)),
                    }
                    for idx in raw_indexes
                    if idx.get("name")
                ],
                "foreign_keys": [
                    {
                        "name": fk.get("name") or "unnamed_fk",
                        "constrained_columns": fk.get("constrained_columns", []),
                        "referred_table": fk.get("referred_table", ""),
                        "referred_columns": fk.get("referred_columns", []),
                    }
                    for fk in raw_fks
                ],
                "partition_key": None,
            }

        except NoSuchTableError:
            logger.warning("Table %r not found; returning empty snapshot.", table_name)
            fields = []
            row_count = None
            snapshot_extra = {
                "schema": schema,
                "full_name": full_name,
            }

        return SchemaSnapshot(
            object_id="",  # Auto-provisioned objects don't have an ID until saved
            object_name=table_name,
            runner_type="database",
            captured_at=captured_at,
            row_count_estimate=row_count,
            fields=fields,
            extra=snapshot_extra,
        )
