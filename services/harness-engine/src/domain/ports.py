"""
Domain Ports — abstract interfaces for external data dependencies.
Application layer depends ONLY on these Protocols, never on SQLAlchemy or file paths.
Implementations live in infrastructure/adapters/.
"""

from __future__ import annotations

from typing import Any, Protocol


class ColumnMetadata(Protocol):
    name: str
    data_type: str
    is_primary_key: bool
    is_indexed: bool
    has_description: bool
    policy_tags: list[str]  # e.g. ["PII", "RESTRICTED"]


class ObjectMetadata(Protocol):
    object_id: str
    asset_id: str
    object_type: str  # TABLE | FILE | API_RESOURCE | VIEW | COLLECTION
    columns: list[ColumnMetadata]
    required_stewards: list[str]
    owner_email: str


class ExecutionMetrics(Protocol):
    object_id: str
    avg_volume_gb: float
    avg_duration_seconds: float
    p95_duration_seconds: float
    last_run_status: str  # success | failed | unknown
    sample_size: int


class MetadataPort(Protocol):
    """Read-only port for platform schema registry."""

    def get_object_metadata(self, asset_id: str, object_id: str) -> ObjectMetadata | None: ...
    def list_objects_for_asset(self, asset_id: str) -> list[ObjectMetadata]: ...


class MetricsPort(Protocol):
    """Read-only port for historical execution metrics."""

    def get_execution_metrics(self, object_id: str) -> ExecutionMetrics | None: ...


class PlatformSchemaPort(Protocol):
    """Porta read-only para buscar JSON Schema do contrato YAML da plataforma."""

    def get_json_schema(self) -> dict[str, Any]: ...


class PlatformExamplesPort(Protocol):
    """Porta read-only para buscar gold examples YAML canônicos da plataforma."""

    def get_gold_examples(self) -> dict[str, str]: ...
