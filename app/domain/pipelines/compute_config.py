from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.domain.pipelines.compute_engine import ComputeEngine


@dataclass(frozen=True)
class ComputeConfig:
    """
    Compute engine for the extraction/transformation job.

    The compute engine is responsible for:
    extract -> canonicalize -> cast_technical_types -> add_processing_timestamp
    -> basic_quality_validations -> write_parquet -> write_schema_json -> write_metrics_json
    """

    engine: ComputeEngine = ComputeEngine.DEFAULT
    num_workers: int = 1
    machine_type: str = "n1-standard-2"
    staging_bucket: str = ""  # GCS/S3 bucket for parquet output
    select: str = ""  # Transformation selection filter: model names, tags, or refs (e.g. 'tag:hourly', 'gold')
    source_type: str = ""
    credential_ref: str = ""
    driver: str = ""
    endpoint: str = ""
    records_path: str = ""
    format: str = ""
    multiline: bool = False

    def to_engine_config(self) -> dict[str, Any]:
        """Produce the unified dictionary of engine-level execution configuration."""
        cfg: dict[str, Any] = {
            "num_workers": self.num_workers,
            "machine_type": self.machine_type,
        }
        if self.source_type:
            cfg["source_type"] = self.source_type
        if self.credential_ref:
            cfg["credential_ref"] = self.credential_ref
        if self.driver:
            cfg["driver"] = self.driver
        if self.endpoint:
            cfg["endpoint"] = self.endpoint
        if self.records_path:
            cfg["records_path"] = self.records_path
        if self.format:
            cfg["format"] = self.format
        if self.multiline:
            cfg["multiline"] = self.multiline
        return cfg
