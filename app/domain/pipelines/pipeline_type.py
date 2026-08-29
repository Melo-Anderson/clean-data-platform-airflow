from __future__ import annotations

from enum import StrEnum


class PipelineType(StrEnum):
    INGESTION = "ingestion"
    ETL = "etl"
    TRANSFORMATION = "transformation"
    EXPORT = "export"
