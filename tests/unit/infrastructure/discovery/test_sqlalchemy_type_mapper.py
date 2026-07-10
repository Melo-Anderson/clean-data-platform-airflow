# tests/unit/infrastructure/discovery/test_sqlalchemy_type_mapper.py
from __future__ import annotations

import pytest
import sqlalchemy.types as T

from app.infrastructure.discovery.sqlalchemy_type_mapper import map_sa_type_to_normalized


@pytest.mark.parametrize(
    "sa_type,expected",
    [
        (T.String(), "string"),
        (T.Text(), "string"),
        (T.Unicode(), "string"),
        (T.UnicodeText(), "string"),
        (T.Integer(), "integer"),
        (T.SmallInteger(), "integer"),
        (T.BigInteger(), "bigint"),
        (T.Float(), "float"),
        (T.Numeric(), "float"),
        (T.Boolean(), "boolean"),
        (T.Date(), "date"),
        (T.DateTime(), "datetime"),
        (T.Time(), "time"),
        (T.JSON(), "json"),
        (T.LargeBinary(), "binary"),
        (T.Uuid(), "uuid"),
        (T.NullType(), "unknown"),
    ],
)
def test_map_sa_type_to_normalized(sa_type: T.TypeEngine, expected: str) -> None:
    assert map_sa_type_to_normalized(sa_type) == expected


def test_unknown_type_returns_unknown() -> None:
    class CustomType(T.TypeEngine):
        pass

    assert map_sa_type_to_normalized(CustomType()) == "unknown"
