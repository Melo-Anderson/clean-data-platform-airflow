from pathlib import Path

from app.infrastructure.adapters.dbt.dbt_config_provider import DbtConfigProvider


def test_dbt_config_provider_loads_project_config() -> None:
    project_dir = Path("dbt_project")
    provider = DbtConfigProvider(project_dir=project_dir)
    config = provider.get_project_config()

    assert config["name"] == "platform_data_platform"
    assert config["vars"]["min_shared_accounts"] == 2
    assert config["vars"]["min_rollover_ratio"] == 0.5
    assert provider.get_target_dataset("silver") == "platform_silver"
    assert provider.get_target_dataset("gold") == "platform_gold"
