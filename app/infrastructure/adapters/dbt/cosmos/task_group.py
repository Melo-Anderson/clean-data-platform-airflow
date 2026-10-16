from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def build_dbt_task_group(
    group_id: str,
    project_dir: str | Path,
    manifest_path: str | Path,
    profiles_yml_path: str | Path,
    select: list[str] | None = None,
    profile_name: str = "platform_bigquery",
    target_name: str = "dev",
    virtualenv_dir: str | Path | None = None,
    operator_args: dict[str, Any] | None = None,
) -> Any:
    """Builds and returns an Astronomer Cosmos DbtTaskGroup.

    Encapsulates Cosmos imports locally to avoid global side-effects.
    """
    from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
    from cosmos.constants import ExecutionMode, LoadMode

    project_config = ProjectConfig(
        dbt_project_path=str(project_dir),
        manifest_path=str(manifest_path),
    )

    profile_config = ProfileConfig(
        profile_name=profile_name,
        target_name=target_name,
        profiles_yml_filepath=str(profiles_yml_path),
    )

    execution_config = ExecutionConfig(
        execution_mode=ExecutionMode.VIRTUALENV if virtualenv_dir else ExecutionMode.LOCAL,
        virtualenv_dir=Path(virtualenv_dir) if virtualenv_dir else None,
    )

    render_config = RenderConfig(
        load_method=LoadMode.DBT_MANIFEST,
        select=select or [],
    )

    return DbtTaskGroup(
        group_id=group_id,
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
        operator_args=operator_args or {},
    )
