from app.application.shared.ports.dwh_provisioner_port import DwhProvisionerPort
from app.config import Settings
from app.infrastructure.dwh_provisioners.bigquery_provisioner import BigQueryProvisioner
from app.infrastructure.dwh_provisioners.noop_provisioner import NoOpDwhProvisioner
from app.infrastructure.dwh_provisioners.registry import DwhProvisionerRegistry


def _create_bigquery_provisioner(s: Settings) -> BigQueryProvisioner:
    """Cria BigQueryProvisioner a partir de Settings tipado, sem getattr."""
    return BigQueryProvisioner(
        project=str(s.dwh.gcp_project or ""),
        cache_ttl_seconds=s.dwh.cache_ttl_seconds,
        credentials_path=s.dwh.resolved_credentials_path,
    )


DwhProvisionerRegistry.register("bigquery", _create_bigquery_provisioner)
DwhProvisionerRegistry.register("noop", lambda _s: NoOpDwhProvisioner())


def get_dwh_provisioner(settings: Settings) -> DwhProvisionerPort:
    """Factory that resolves the active DwhProvisionerAdapter from environment configuration.

    Follows the same pattern as get_catalog_adapter and get_dwh_loader:
    zero hardcoded values, all configuration delegated to Settings.

    Supported adapters:
        - "bigquery": Provisions Datasets and Tables in Google BigQuery via ADC.
        - "noop" (default): No-op stub, safe for local dev without GCP credentials.
    """
    return DwhProvisionerRegistry.get(settings)
