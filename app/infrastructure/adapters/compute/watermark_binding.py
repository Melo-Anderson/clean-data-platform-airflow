from __future__ import annotations

import re
from typing import Any

from app.domain.pipelines.watermark import Watermark

_IDENTIFIER_REGEX = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
WATERMARK_PLACEHOLDER = ":watermark_value"


def bind_sql_placeholder(query: str, watermark: Watermark | None) -> str:
    """Replace :watermark_value placeholder in custom SQL queries with typed literal.

    Raises ValueError if placeholder is present and watermark is None.
    """
    if WATERMARK_PLACEHOLDER in query:
        if watermark is None:
            raise ValueError("Query requires :watermark_value but no watermark state is available")
        return query.replace(WATERMARK_PLACEHOLDER, watermark.as_sql_literal())
    return query


def watermark_predicate(column: str, watermark: Watermark | None) -> str | None:
    """Construct a SQL WHERE predicate for the incremental column.

    Validates column name against identifier regex to prevent SQL injection.
    Returns None if watermark is None.
    """
    if not _IDENTIFIER_REGEX.match(column):
        raise ValueError(f"Invalid column identifier for watermark predicate: '{column}'")
    if watermark is None:
        return None
    return f"{column} > {watermark.as_sql_literal()}"


def bind_request_params(params: dict[str, Any], watermark: Watermark | None) -> dict[str, Any]:
    """Replace :watermark_value placeholder in request params with raw param value.

    Raises ValueError if placeholder is present and watermark is None.
    """
    bound: dict[str, Any] = {}
    for key, val in params.items():
        if isinstance(val, str) and WATERMARK_PLACEHOLDER in val:
            if watermark is None:
                raise ValueError(
                    "Params require :watermark_value but no watermark state is available"
                )
            if val == WATERMARK_PLACEHOLDER:
                bound[key] = watermark.as_param_value()
            else:
                bound[key] = val.replace(WATERMARK_PLACEHOLDER, watermark.as_param_value())
        else:
            bound[key] = val
    return bound
