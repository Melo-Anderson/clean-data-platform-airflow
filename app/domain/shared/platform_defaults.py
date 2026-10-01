from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

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
    decimal_scale: int = 2
    decimal_overflow: Literal["round", "fail"] = "round"


@dataclass(frozen=True)
class StorageDefaults:
    format: str = "csv"
    delimiter: str = ","
    quote_char: str = '"'
    multiline: bool = False
    compression: str = "none"
    chunk_size_bytes: int = 67108864  # 64 MB


@dataclass(frozen=True)
class DatabaseDefaults:
    batch_size: int = 2500
    num_partitions: int = 0
    max_open_conns: int = 4
    max_idle_conns: int = 2
    conn_max_lifetime_s: int = 300
    flatten_nested: bool = False


@dataclass(frozen=True)
class RestApiDefaults:
    http_method: str = "GET"
    pagination_type: str = "page_number"
    page_param: str = "page"
    size_param: str = "size"
    initial_page: int = 1
    max_pages_limit: int = 10000
    rate_limit_rps: float = 50.0
    timeout_ms: int = 30000
    max_retries: int = 3
    initial_backoff_ms: int = 500
    max_backoff_ms: int = 10000
    backoff_multiplier: float = 2.0
    circuit_breaker_fails: int = 5
    circuit_breaker_timeout_ms: int = 10000
    half_open_limit: int = 1


@dataclass(frozen=True)
class DestinationDefaults:
    output_format: str = "parquet"
    single_file: bool = False
    include_audit_columns: bool = False
    bigquery_write_disposition: str = "WRITE_APPEND"
    bigquery_batch_size: int = 500
    rest_api_method: str = "POST"
    rest_api_batch_size: int = 100
    rest_api_rate_limit_rps: int = 50
    rest_api_timeout_ms: int = 30000
    rest_api_max_retries: int = 3


@dataclass(frozen=True)
class DlqDefaults:
    enabled: bool = True
    max_error_percentage: float = 0.0


@dataclass(frozen=True)
class OmniBeamDefaults:
    pipeline_type: str = "ingestion"
    runner: str = "direct"


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
    storage: StorageDefaults = field(default_factory=StorageDefaults)
    database: DatabaseDefaults = field(default_factory=DatabaseDefaults)
    rest_api: RestApiDefaults = field(default_factory=RestApiDefaults)
    destination: DestinationDefaults = field(default_factory=DestinationDefaults)
    dlq: DlqDefaults = field(default_factory=DlqDefaults)
    omnibeam: OmniBeamDefaults = field(default_factory=OmniBeamDefaults)

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
            storage=StorageDefaults(),
            database=DatabaseDefaults(),
            rest_api=RestApiDefaults(),
            destination=DestinationDefaults(),
            dlq=DlqDefaults(),
            omnibeam=OmniBeamDefaults(),
        )
