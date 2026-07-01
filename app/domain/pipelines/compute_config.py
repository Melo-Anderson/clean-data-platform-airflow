from __future__ import annotations

from dataclasses import dataclass

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
