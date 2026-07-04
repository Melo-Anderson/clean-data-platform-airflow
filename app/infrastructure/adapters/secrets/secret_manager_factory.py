from __future__ import annotations

from app.application.shared.secret_manager_port import SecretManagerPort
from app.config import Settings
from app.infrastructure.adapters.secrets.noop_secret_manager_adapter import NoopSecretManagerAdapter
from app.infrastructure.adapters.secrets.bao_secret_manager_adapter import BaoSecretManagerAdapter


def get_secret_manager(settings: Settings) -> SecretManagerPort:
    """
    Factory to instantiate the appropriate SecretManagerPort based on configuration.
    """
    if settings.secret_manager_adapter.lower() == "openbao":
        if not settings.vault_url or not settings.vault_token:
            raise ValueError("vault_url and vault_token must be set when using openbao adapter")
        return BaoSecretManagerAdapter(
            vault_url=settings.vault_url,
            vault_token=settings.vault_token
        )
        
    # Default to Noop for local development if not configured
    return NoopSecretManagerAdapter()
