from __future__ import annotations

from unittest.mock import MagicMock

from app.infrastructure.discovery.profiler_strategy import (
    DefaultProfilerStrategy,
    OracleProfilerStrategy,
    PostgresProfilerStrategy,
    get_profiler_strategy,
)


def test_factory_returns_correct_strategy() -> None:
    assert isinstance(get_profiler_strategy("postgresql"), PostgresProfilerStrategy)
    assert isinstance(get_profiler_strategy("oracle"), OracleProfilerStrategy)
    assert isinstance(get_profiler_strategy("mysql"), DefaultProfilerStrategy)
    assert isinstance(get_profiler_strategy("sqlite"), DefaultProfilerStrategy)


def test_default_profiler_executes_count_query() -> None:
    sync_conn = MagicMock()
    mock_row = (42,)
    sync_conn.execute.return_value.fetchone.return_value = mock_row

    strategy = DefaultProfilerStrategy()
    count = strategy.estimate_row_count(sync_conn, "users", "public")

    assert count == 42
    sync_conn.execute.assert_called_once()
    sql_text = str(sync_conn.execute.call_args[0][0])
    assert 'SELECT COUNT(*) FROM "public"."users"' in sql_text


def test_default_profiler_handles_exception_gracefully() -> None:
    sync_conn = MagicMock()
    sync_conn.execute.side_effect = Exception("DB error")

    strategy = DefaultProfilerStrategy()
    count = strategy.estimate_row_count(sync_conn, "users", None)

    assert count is None
