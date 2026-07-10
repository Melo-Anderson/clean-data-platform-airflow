from __future__ import annotations

import json
import logging

from app.infrastructure.logging_config import configure_logging


def test_configure_logging_emits_json(capsys) -> None:
    # Set json_output to True
    configure_logging(log_level="DEBUG", json_output=True)
    logger = logging.getLogger("test.pipeline")
    logger.info("pipeline.triggered", extra={"pipeline_id": "p-001"})

    captured = capsys.readouterr()
    output = captured.out or captured.err
    assert output != ""

    # Verify the output is valid JSON and contains our structured fields
    data = json.loads(output.strip())
    assert data["event"] == "pipeline.triggered"
    assert data["logger"] == "test.pipeline"
    assert data["level"] == "info"
    assert data["pipeline_id"] == "p-001"
    assert "timestamp" in data
