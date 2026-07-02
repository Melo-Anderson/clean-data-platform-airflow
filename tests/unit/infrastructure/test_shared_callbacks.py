from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from app.infrastructure.airflow_callbacks.shared_callbacks import (
    check_dependencies,
)


def test_check_dependencies_raises_when_upstream_not_satisfied() -> None:
    mock_client = MagicMock()
    mock_client.pipeline_succeeded_on.return_value = False

    with patch("app.infrastructure.platform_client.get_platform_client", return_value=mock_client):
        with pytest.raises(RuntimeError, match="Dependency not satisfied"):
            check_dependencies(
                pipeline_id="pipe-1",
                depends_on=[{"pipeline_id": "upstream-1", "dependency_type": "dataset"}],
                logical_date=datetime.now(tz=UTC),
            )


def test_check_dependencies_passes_when_all_satisfied() -> None:
    mock_client = MagicMock()
    mock_client.pipeline_succeeded_on.return_value = True

    with patch("app.infrastructure.platform_client.get_platform_client", return_value=mock_client):
        result = check_dependencies(
            pipeline_id="pipe-1",
            depends_on=[{"pipeline_id": "upstream-1", "dependency_type": "dataset"}],
            logical_date=datetime.now(tz=UTC),
        )
        assert result["dependencies_ok"] is True
