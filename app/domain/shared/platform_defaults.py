from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.config import Settings


@dataclass(frozen=True)
class AirflowDefaults:
    retries: int = 3
    retry_delay_minutes: int = 5
    execution_timeout_minutes: int = 120
    sla_minutes: int = 90
    pool: str = "default_pool"


@dataclass(frozen=True)
class ExtractionDefaults:
    page_size: int = 1000
    compression: str = "snappy"
    encoding: str = "utf-8"
    load_strategy: str = "full_load"


@dataclass(frozen=True)
class ComputeDefaults:
    num_workers: int = 1
    machine_type: str = "n1-standard-2"
    default_engine: str = "duckdb"


@dataclass(frozen=True)
class PlatformDefaults:
    airflow: AirflowDefaults = field(default_factory=AirflowDefaults)
    extraction: ExtractionDefaults = field(default_factory=ExtractionDefaults)
    compute: ComputeDefaults = field(default_factory=ComputeDefaults)

    @classmethod
    def from_settings(cls, settings: Settings) -> PlatformDefaults:
        """Unico ponto que le defaults operacionais de Settings."""
        return cls(
            airflow=AirflowDefaults(
                retries=settings.airflow.default_retries,
                retry_delay_minutes=settings.airflow.default_retry_delay_minutes,
                execution_timeout_minutes=settings.airflow.default_execution_timeout_minutes,
                sla_minutes=settings.airflow.default_sla_minutes,
                pool=settings.airflow.default_pool,
            ),
            extraction=ExtractionDefaults(
                page_size=settings.default_page_size,
                compression=settings.default_compression,
                encoding=settings.default_encoding,
                load_strategy=settings.default_load_strategy,
            ),
            compute=ComputeDefaults(
                num_workers=settings.compute.default_num_workers,
                machine_type=settings.compute.default_machine_type,
                default_engine=settings.compute.default_engine,
            ),
        )
