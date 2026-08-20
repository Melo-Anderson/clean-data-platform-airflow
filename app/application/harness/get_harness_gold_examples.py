from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import yaml

from app.application.shared.ports.generator_ports import YamlGeneratorPort
from app.application.unit_of_work import UnitOfWork

_TEMPLATES_DIR = Path(__file__).parent.parent.parent / "infrastructure" / "harness" / "templates"


def _load_fallback_template(
    pipeline_type: str, templates_dir: Path | None = None
) -> dict[str, Any]:
    """Load a canonical fallback YAML template from disk."""
    base_dir = templates_dir or _TEMPLATES_DIR
    template_path = base_dir / f"{pipeline_type}.yaml"
    if not template_path.exists():
        template_path = base_dir / "ingestion.yaml"
    with template_path.open(encoding="utf-8") as f:
        return cast(dict[str, Any], yaml.safe_load(f))


class GetHarnessGoldExamplesUseCase:
    """Return real or canonical pipeline YAML examples for the Harness Engine."""

    def __init__(
        self,
        uow: UnitOfWork | None = None,
        yaml_generator: YamlGeneratorPort | None = None,
        templates_dir: Path | None = None,
    ) -> None:
        self._uow = uow
        self._yaml_generator = yaml_generator
        self._templates_dir = templates_dir

    async def execute(
        self,
        pipeline_type: str,
        compute_engine: str | None = None,
        transform_engine: str | None = None,
        source_asset_id: str | None = None,
        limit: int = 3,
    ) -> dict[str, Any]:
        """Return up to `limit` YAML examples for the given pipeline type.

        Args:
            pipeline_type: Required. One of 'ingestion', 'etl', 'export'.
            compute_engine: Optional. Filter by compute engine (e.g. 'spark', 'duckdb').
            transform_engine: Optional. Filter by transform engine (e.g. 'dbt').
            source_asset_id: Optional. Filter by source asset ID.
            limit: Maximum number of examples to return. Default 3.

        Returns:
            Dict with keys: pipeline_type, total_count, examples (list of {pipeline_id, yaml_snippet}).
        """
        examples: list[dict[str, str]] = []

        if self._uow:
            pipelines = await self._uow.pipelines.find_all()
            filtered = [
                p
                for p in pipelines
                if p.type.value == pipeline_type
                and (not compute_engine or p.compute.engine.value == compute_engine)
                and (not transform_engine or p.transform.engine.value == transform_engine)
                and (not source_asset_id or p.source_asset == source_asset_id)
            ][:limit]
            for p in filtered:
                if self._yaml_generator:
                    yaml_snippet = self._yaml_generator.generate(p)
                    examples.append({"pipeline_id": p.id, "yaml_snippet": yaml_snippet})

        if not examples:
            fallback = _load_fallback_template(pipeline_type, self._templates_dir)
            examples.append(
                {
                    "pipeline_id": str(fallback["pipeline_id"]),
                    "yaml_snippet": yaml.dump(fallback, sort_keys=False, allow_unicode=True),
                }
            )

        return {
            "pipeline_type": pipeline_type,
            "total_count": len(examples),
            "examples": examples[:limit],
        }
