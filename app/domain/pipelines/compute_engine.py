from __future__ import annotations

from enum import StrEnum


class ComputeEngine(StrEnum):
    DUCKDB = "duckdb"
    SPARK = "spark"
    DATAFLOW = "dataflow"
    DEFAULT = "default"
    REST_API = "rest_api"
