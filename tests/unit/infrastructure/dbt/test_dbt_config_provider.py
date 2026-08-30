from pathlib import Path

import pytest
import yaml

from app.infrastructure.adapters.dbt.dbt_config_provider import DbtConfigProvider


def test_dbt_config_provider_with_tmp_project(tmp_path: Path) -> None:
    project_file = tmp_path / "dbt_project.yml"
    project_file.write_text(
        yaml.dump(
            {
                "name": "platform_data_platform",
                "vars": {
                    "min_shared_accounts": 2,
                    "min_rollover_ratio": 0.5,
                    "silver_dataset": "platform_silver",
                    "gold_dataset": "platform_gold",
                },
            }
        ),
        encoding="utf-8",
    )
    provider = DbtConfigProvider(project_dir=tmp_path)
    config = provider.get_project_config()
    assert config["name"] == "platform_data_platform"
    assert config["vars"]["min_shared_accounts"] == 2
    assert config["vars"]["min_rollover_ratio"] == 0.5
    assert provider.get_target_dataset("silver") == "platform_silver"
    assert provider.get_target_dataset("gold") == "platform_gold"


@pytest.mark.skipif(
    not Path("dbt_project/dbt_project.yml").exists(),
    reason="dbt_project is gitignored and not present in CI checkout environment",
)
def test_dbt_config_provider_loads_project_config() -> None:
    project_dir = Path("dbt_project")
    provider = DbtConfigProvider(project_dir=project_dir)
    config = provider.get_project_config()

    assert config["name"] == "platform_data_platform"
    assert config["vars"]["min_shared_accounts"] == 2
    assert config["vars"]["min_rollover_ratio"] == 0.5
    assert provider.get_target_dataset("silver") == "platform_silver"
    assert provider.get_target_dataset("gold") == "platform_gold"
