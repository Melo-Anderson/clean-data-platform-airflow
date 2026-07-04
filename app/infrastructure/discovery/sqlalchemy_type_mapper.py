# app/infrastructure/discovery/sqlalchemy_type_mapper.py
from __future__ import annotations

import sqlalchemy.types as T

# Resolution order matters: more specific types (BigInteger before Integer)
# must precede their parents to avoid incorrect downgrading.
_TYPE_MAP: tuple[tuple[type[T.TypeEngine], str], ...] = (
    (T.BigInteger, "bigint"),
    (T.Boolean, "boolean"),
    (T.Date, "date"),
    (T.DateTime, "datetime"),
    (T.Float, "float"),
    (T.Integer, "integer"),
    (T.JSON, "json"),
    (T.LargeBinary, "binary"),
    (T.Numeric, "float"),
    (T.String, "string"),
    (T.Text, "string"),
    (T.Time, "time"),
    (T.Unicode, "string"),
    (T.UnicodeText, "string"),
    (T.Uuid, "uuid"),
)


def map_sa_type_to_normalized(sa_type: T.TypeEngine) -> str:
    """
    Map a SQLAlchemy TypeEngine instance to the platform's canonical normalized type string.

    Returns "unknown" for any type not in the mapping (e.g., dialect-specific types
    that don't inherit from a recognized generic class).
    """
    for base_type, normalized in _TYPE_MAP:
        if isinstance(sa_type, base_type):
            return normalized
    return "unknown"
