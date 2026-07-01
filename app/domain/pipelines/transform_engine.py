from __future__ import annotations

from enum import StrEnum


class TransformEngine(StrEnum):
    DBT = "dbt"
    DATAFORM = "dataform"
    NONE = "none"
