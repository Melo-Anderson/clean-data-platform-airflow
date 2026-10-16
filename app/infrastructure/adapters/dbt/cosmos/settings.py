from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CosmosDbtSettings:
    """Immutable configuration settings for Cosmos dbt execution.

    Adheres to Fail-Fast principles without magic fallback heuristics.
    """

    project_dir: Path
    profiles_dir: Path
    manifest_path: Path
    virtualenv_dir: Path | None = None
    profile_name: str = "platform_bigquery"
    target_name: str = "dev"

    def validate(self) -> None:
        if not self.project_dir.exists():
            raise FileNotFoundError(f"dbt project_dir does not exist: {self.project_dir}")
        if not self.profiles_dir.exists():
            raise FileNotFoundError(f"dbt profiles_dir does not exist: {self.profiles_dir}")
        if not self.manifest_path.exists():
            raise FileNotFoundError(f"dbt manifest.json does not exist: {self.manifest_path}")
