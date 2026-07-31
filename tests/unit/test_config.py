from __future__ import annotations

import os
from unittest.mock import patch

from app.config import get_settings


def test_settings_has_gcp_defaults():
    get_settings.cache_clear()
    settings = get_settings()
    assert hasattr(settings, "gcp_project")
    assert settings.gcp_project == ""
    assert hasattr(settings, "dwh_provisioner_adapter")
    assert settings.dwh_provisioner_adapter == "noop"


def test_settings_reads_gcp_env_vars():
    get_settings.cache_clear()
    with patch.dict(
        os.environ,
        {
            "PLATFORM_GCP_PROJECT": "test-gcp-project",
            "PLATFORM_DWH_PROVISIONER_ADAPTER": "bigquery",
        },
    ):
        settings = get_settings()
        assert settings.gcp_project == "test-gcp-project"
        assert settings.dwh_provisioner_adapter == "bigquery"
    get_settings.cache_clear()
