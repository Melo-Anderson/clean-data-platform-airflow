from __future__ import annotations

from app.application.shared.ports.dwh_provisioner_port import DwhProvisionerPort
from app.config import Settings
from app.infrastructure.dwh_provisioners.bigquery_provisioner import BigQueryProvisioner
from app.infrastructure.dwh_provisioners.noop_provisioner import NoOpDwhProvisioner


def get_dwh_provisioner(settings: Settings) -> DwhProvisionerPort:
    """Factory that resolves the active DwhProvisionerAdapter from environment configuration.

    Follows the same pattern as get_catalog_adapter and get_dwh_loader:
    zero hardcoded values, all configuration delegated to Settings.

    Supported adapters:
        - "bigquery": Provisions Datasets and Tables in Google BigQuery via ADC.
        - "noop" (default): No-op stub, safe for local dev without GCP credentials.
    """
    adapter_name = settings.dwh_provisioner_adapter.lower()

    if adapter_name == "bigquery":
        return BigQueryProvisioner(project=settings.gcp_project or None)

    return NoOpDwhProvisioner()
