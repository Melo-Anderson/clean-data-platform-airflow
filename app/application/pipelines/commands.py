# app/application/pipelines/commands.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RegisterPipelineCommand:
    """Command object for RegisterPipelineUseCase.

    Encapsulates all inputs required to register a new pipeline.
    Frozen to prevent accidental mutation after construction.
    """

    name: str
    pipeline_type: str
    owner_email: str
    source_asset: str = ""
    cron_schedule: str = ""
    destination_asset: str = ""
    destination_objects: list[dict[str, Any]] | None = None
    source_objects: list[dict[str, Any]] | None = None
    compute: dict[str, Any] | None = None
    quality_rules: list[dict[str, Any]] | None = None
    airflow_config: dict[str, Any] | None = None
