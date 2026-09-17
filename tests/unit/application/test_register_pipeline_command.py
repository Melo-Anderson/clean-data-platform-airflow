# tests/unit/application/test_register_pipeline_command.py
from __future__ import annotations

import pytest

from app.application.pipelines.commands import RegisterPipelineCommand


def test_register_pipeline_command_is_frozen_dataclass() -> None:
    """RegisterPipelineCommand deve ser imutavel (frozen=True)."""
    cmd = RegisterPipelineCommand(
        name="my-pipeline",
        pipeline_type="ingestion",
        owner_email="owner@example.com",
    )
    with pytest.raises((AttributeError, TypeError)):
        cmd.name = "other"  # type: ignore[misc]


def test_register_pipeline_command_requires_name_and_type_and_owner() -> None:
    """RegisterPipelineCommand deve falhar se campos obrigatorios estiverem ausentes."""
    with pytest.raises(TypeError):
        RegisterPipelineCommand()  # type: ignore[call-arg]


def test_register_pipeline_command_has_sensible_defaults() -> None:
    """Campos opcionais devem ter defaults que permitem criacao minima."""
    cmd = RegisterPipelineCommand(
        name="pipe",
        pipeline_type="ingestion",
        owner_email="x@y.com",
    )
    assert cmd.source_asset == ""
    assert cmd.cron_schedule == ""
    assert cmd.destination_objects is None
    assert cmd.source_objects is None
