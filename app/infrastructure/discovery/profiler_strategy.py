# app/infrastructure/discovery/profiler_strategy.py
from __future__ import annotations

import logging
from typing import Protocol

from sqlalchemy import Connection, text

logger = logging.getLogger(__name__)


class DatabaseProfilerStrategy(Protocol):
    """Interface for database-specific profiling operations."""

    def estimate_row_count(
        self, sync_conn: Connection, table_name: str, schema: str | None
    ) -> int | None: ...


class PostgresProfilerStrategy:
    """Uses pg_class catalog statistics — near-instant, no sequential scan."""

    def estimate_row_count(
        self, sync_conn: Connection, table_name: str, schema: str | None
    ) -> int | None:
        try:
            if schema:
                sql = text("""
                    SELECT reltuples::bigint
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = :table AND n.nspname = :schema
                """)
                row = sync_conn.execute(sql, {"table": table_name, "schema": schema}).fetchone()
            else:
                sql = text("SELECT reltuples::bigint FROM pg_class WHERE relname = :table")
                row = sync_conn.execute(sql, {"table": table_name}).fetchone()
            if row and row[0] is not None and row[0] >= 0:
                return int(row[0])
        except Exception:
            logger.debug("pg_class lookup failed for %r, falling back to COUNT(*)", table_name)
        return DefaultProfilerStrategy().estimate_row_count(sync_conn, table_name, schema)


class OracleProfilerStrategy:
    """Uses ALL_TABLES.NUM_ROWS — populated after DBMS_STATS.GATHER_TABLE_STATS or ANALYZE.

    NOTE: NUM_ROWS is an estimate from the last statistics collection, not a live count.
    Returns None if statistics are stale (NUM_ROWS IS NULL) and falls back to COUNT(*).
    """

    def estimate_row_count(
        self, sync_conn: Connection, table_name: str, schema: str | None
    ) -> int | None:
        try:
            owner = schema.upper() if schema else None
            if owner:
                sql = text("""
                    SELECT NUM_ROWS
                    FROM ALL_TABLES
                    WHERE TABLE_NAME = :table AND OWNER = :schema
                """)
                row = sync_conn.execute(
                    sql, {"table": table_name.upper(), "schema": owner}
                ).fetchone()
            else:
                sql = text("""
                    SELECT NUM_ROWS
                    FROM USER_TABLES
                    WHERE TABLE_NAME = :table
                """)
                row = sync_conn.execute(sql, {"table": table_name.upper()}).fetchone()
            if row and row[0] is not None:
                return int(row[0])
        except Exception:
            logger.debug("ALL_TABLES lookup failed for %r, falling back to COUNT(*)", table_name)
        return DefaultProfilerStrategy().estimate_row_count(sync_conn, table_name, schema)


class DefaultProfilerStrategy:
    """Generic fallback — works on any SQL database but may trigger a full scan."""

    def estimate_row_count(
        self, sync_conn: Connection, table_name: str, schema: str | None
    ) -> int | None:
        try:
            full = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
            row = sync_conn.execute(text(f"SELECT COUNT(*) FROM {full}")).fetchone()  # noqa: S608
            return int(row[0]) if row else None
        except Exception:
            logger.debug("COUNT(*) failed for %r", table_name, exc_info=True)
            return None


def get_profiler_strategy(dialect_name: str) -> DatabaseProfilerStrategy:
    """Factory: returns the most efficient profiler strategy for the given SQL dialect.

    Extend here to add MySQL, SQL Server, BigQuery, etc.
    """
    strategies: dict[str, DatabaseProfilerStrategy] = {
        "postgresql": PostgresProfilerStrategy(),
        "oracle": OracleProfilerStrategy(),
    }
    return strategies.get(dialect_name, DefaultProfilerStrategy())
