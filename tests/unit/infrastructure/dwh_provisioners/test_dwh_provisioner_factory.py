from __future__ import annotations

from unittest.mock import MagicMock

from app.infrastructure.dwh_provisioners.dwh_provisioner_factory import get_dwh_provisioner


def test_get_dwh_provisioner_returns_noop_by_default():
    settings = MagicMock()
    settings.dwh_provisioner_adapter = "noop"
    settings.gcp_project = ""
    from app.infrastructure.dwh_provisioners.noop_provisioner import NoOpDwhProvisioner

    provisioner = get_dwh_provisioner(settings)
    assert isinstance(provisioner, NoOpDwhProvisioner)


def test_get_dwh_provisioner_returns_bigquery_when_configured():
    settings = MagicMock()
    settings.dwh_provisioner_adapter = "bigquery"
    settings.gcp_project = "my-project"
    from app.infrastructure.dwh_provisioners.bigquery_provisioner import BigQueryProvisioner

    provisioner = get_dwh_provisioner(settings)
    assert isinstance(provisioner, BigQueryProvisioner)


def test_get_dwh_provisioner_bigquery_passes_project():
    settings = MagicMock()
    settings.dwh_provisioner_adapter = "bigquery"
    settings.gcp_project = "personal-project-504117"
    from app.infrastructure.dwh_provisioners.bigquery_provisioner import BigQueryProvisioner

    provisioner = get_dwh_provisioner(settings)
    assert isinstance(provisioner, BigQueryProvisioner)
    assert provisioner._project == "personal-project-504117"


def test_get_dwh_provisioner_raises_for_unknown_adapter():
    import pytest

    settings = MagicMock()
    settings.dwh_provisioner_adapter = "unsupported_adapter"
    with pytest.raises(ValueError, match="Unsupported DWH provisioner adapter"):
        get_dwh_provisioner(settings)
