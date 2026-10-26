from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from typing import Any


class WatermarkType(StrEnum):
    """Supported scalar types for incremental watermarks and pagination offsets."""

    INTEGER = "integer"
    FLOAT = "float"
    TIMESTAMP = "timestamp"
    DATE = "date"
    STRING = "string"


@dataclass(frozen=True, slots=True)
class Watermark:
    """Canonical domain representation of an incremental watermark state.

    Immutable value object encapsulating validation, SQL literal rendering,
    and HTTP parameter stringification.
    """

    value: str
    type: WatermarkType

    @classmethod
    def parse(cls, raw: Any, type_: WatermarkType) -> Watermark:
        """Parse and validate raw input against the declared WatermarkType.

        Fails fast without guessing or falling back to defaults.
        """
        if raw is None or (isinstance(raw, str) and not raw.strip()):
            raise ValueError(f"Watermark value cannot be empty for type '{type_.value}'.")

        val_str = str(raw).strip()

        if type_ == WatermarkType.INTEGER:
            try:
                int(val_str)
            except ValueError as exc:
                raise ValueError(f"Cannot parse '{val_str}' as integer.") from exc

        elif type_ == WatermarkType.FLOAT:
            try:
                float(val_str)
            except ValueError as exc:
                raise ValueError(f"Cannot parse '{val_str}' as float.") from exc

        elif type_ == WatermarkType.TIMESTAMP:
            try:
                datetime.fromisoformat(val_str)
            except ValueError as exc:
                raise ValueError(
                    f"Cannot parse '{val_str}' as timestamp. Must be valid ISO-8601."
                ) from exc

        elif type_ == WatermarkType.DATE:
            try:
                date.fromisoformat(val_str)
            except ValueError as exc:
                raise ValueError(
                    f"Cannot parse '{val_str}' as date. Must be valid ISO-8601 (YYYY-MM-DD)."
                ) from exc

        elif type_ == WatermarkType.STRING:
            pass

        return cls(value=val_str, type=type_)

    def as_sql_literal(self) -> str:
        """Render safely as an escaped SQL literal."""
        if self.type in (WatermarkType.INTEGER, WatermarkType.FLOAT):
            return self.value

        escaped = self.value.replace("'", "''")
        return f"'{escaped}'"

    def as_param_value(self) -> str:
        """Render as a parameter value string for REST API queries."""
        return self.value
