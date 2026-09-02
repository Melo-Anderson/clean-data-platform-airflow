from __future__ import annotations

from unittest.mock import MagicMock

from fastapi import FastAPI

from app.infrastructure.telemetry import setup_telemetry


def test_setup_telemetry_accepts_injected_parameters() -> None:
    app = FastAPI()
    mock_exporter = MagicMock()
    # Shouldn't raise errors when called with injected exporter and parameters
    setup_telemetry(app, service_name="test-service", otlp_endpoint=None, exporter=mock_exporter)
