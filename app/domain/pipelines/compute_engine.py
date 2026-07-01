from __future__ import annotations

from enum import StrEnum


class ComputeEngine(StrEnum):
    SPARK = "spark"
    DATAFLOW = "dataflow"
    DEFAULT = "default"
