from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


class DbtConfigProvider:
    """Provides project configuration, variables, and dataset targets from dbt_project.yml."""

    def __init__(self, project_dir: Path | str = "dbt_project") -> None:
        self._project_dir = Path(project_dir)
        self._config: dict[str, Any] | None = None

    def get_project_config(self) -> dict[str, Any]:
        if self._config is None:
            config_path = self._project_dir / "dbt_project.yml"
            if not config_path.exists():
                raise FileNotFoundError(f"dbt project file not found at {config_path}")
            with config_path.open("r", encoding="utf-8") as f:
                self._config = yaml.safe_load(f) or {}
        return self._config

    def get_target_dataset(self, layer: str) -> str:
        config = self.get_project_config()
        vars_dict = config.get("vars", {})
        key = f"{layer.lower()}_dataset"
        if key in vars_dict:
            return str(vars_dict[key])
        return f"OTG_{layer.lower()}"
