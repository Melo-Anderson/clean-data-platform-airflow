from __future__ import annotations

import pytest

from app.domain.pipelines.watermark import Watermark, WatermarkType
from app.infrastructure.adapters.compute.watermark_binding import (
    bind_request_params,
    bind_sql_placeholder,
    watermark_predicate,
)


def test_bind_sql_placeholder_replaces_token() -> None:
    query = "SELECT * FROM t WHERE ts > :watermark_value AND status = 'active'"
    wm = Watermark.parse("2026-10-24T00:00:00", WatermarkType.TIMESTAMP)
    bound = bind_sql_placeholder(query, wm)
    assert bound == "SELECT * FROM t WHERE ts > '2026-10-24T00:00:00' AND status = 'active'"


def test_bind_sql_placeholder_escapes_single_quotes_anti_injection() -> None:
    query = "SELECT * FROM t WHERE code > :watermark_value"
    wm = Watermark.parse("val'--", WatermarkType.STRING)
    bound = bind_sql_placeholder(query, wm)
    assert bound == "SELECT * FROM t WHERE code > 'val''--'"


def test_bind_sql_placeholder_unmodified_when_no_placeholder() -> None:
    query = "SELECT * FROM t WHERE status = 'active'"
    wm = Watermark.parse("100", WatermarkType.INTEGER)
    assert bind_sql_placeholder(query, wm) == query


def test_bind_sql_placeholder_missing_watermark_raises_value_error() -> None:
    query = "SELECT * FROM t WHERE id > :watermark_value"
    with pytest.raises(
        ValueError, match="Query requires :watermark_value but no watermark state is available"
    ):
        bind_sql_placeholder(query, None)


def test_watermark_predicate_generates_valid_expression() -> None:
    wm = Watermark.parse(100, WatermarkType.INTEGER)
    assert watermark_predicate("order_id", wm) == "order_id > 100"


def test_watermark_predicate_returns_none_when_no_watermark() -> None:
    assert watermark_predicate("order_id", None) is None


def test_watermark_predicate_invalid_column_rejects_injection() -> None:
    wm = Watermark.parse(100, WatermarkType.INTEGER)
    with pytest.raises(ValueError, match="Invalid column identifier for watermark predicate"):
        watermark_predicate("order_id; DROP TABLE users;--", wm)

    with pytest.raises(ValueError, match="Invalid column identifier for watermark predicate"):
        watermark_predicate("invalid-column!", wm)


def test_bind_request_params_replaces_placeholder() -> None:
    params = {"limit": 100, "since": ":watermark_value", "filter": "active"}
    wm = Watermark.parse("2026-10-24", WatermarkType.DATE)
    result = bind_request_params(params, wm)
    assert result == {"limit": 100, "since": "2026-10-24", "filter": "active"}


def test_bind_request_params_missing_watermark_raises_value_error() -> None:
    params = {"limit": 100, "since": ":watermark_value"}
    with pytest.raises(
        ValueError, match="Params require :watermark_value but no watermark state is available"
    ):
        bind_request_params(params, None)
