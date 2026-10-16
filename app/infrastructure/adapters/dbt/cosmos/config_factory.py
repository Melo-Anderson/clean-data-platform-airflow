from __future__ import annotations

from typing import Any

from app.infrastructure.adapters.dbt.cosmos.settings import CosmosDbtSettings


def build_cosmos_configs(
    settings: CosmosDbtSettings,
    pipeline_config: dict[str, Any],
) -> dict[str, Any]:
    """Builds kwargs and configuration dicts for Cosmos DbtTaskGroup / operators.

    Returns clean configuration parameters adhering to Cosmos contracts.
    """
    settings.validate()

    select_filter = pipeline_config.get("select") or pipeline_config.get("select_models") or ""
    select_list = [s.strip() for s in select_filter.split() if s.strip()] if select_filter else []

    profiles_yml = settings.profiles_dir / "profiles.yml"
    if not profiles_yml.exists():
        profiles_yml = settings.project_dir / "profiles.yml"

    return {
        "project_dir": str(settings.project_dir),
        "manifest_path": str(settings.manifest_path),
        "profiles_yml_path": str(profiles_yml),
        "profile_name": settings.profile_name,
        "target_name": settings.target_name,
        "select": select_list,
        "virtualenv_dir": str(settings.virtualenv_dir) if settings.virtualenv_dir else None,
    }
