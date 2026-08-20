from __future__ import annotations

from app.application.shared.ports.generator_ports import YamlGeneratorPort
from app.application.unit_of_work import UnitOfWork
from app.domain.shared.exceptions import PlatformNotFoundError


class GetPipelineYamlUseCase:
    """Retrieve the canonical self-healed YAML representation of an existing pipeline.

    Returns a plain dict (not a Pydantic schema) to respect Clean Architecture boundaries.
    The router is responsible for wrapping the result into an HTTP response schema.
    """

    def __init__(
        self,
        uow: UnitOfWork,
        yaml_generator: YamlGeneratorPort | None = None,
    ) -> None:
        self._uow = uow
        self._yaml_generator = yaml_generator

    async def execute(self, pipeline_id: str) -> dict[str, str]:
        """Execute pipeline YAML export.

        Args:
            pipeline_id: The pipeline unique identifier (matches dag_id in Airflow).

        Returns:
            Dict with keys 'pipeline_id' and 'pipeline_yaml'.

        Raises:
            PlatformNotFoundError: If no pipeline is found for the given ID.
        """
        pipeline = await self._uow.pipelines.find_by_id(pipeline_id)
        if pipeline is None:
            raise PlatformNotFoundError(f"Pipeline not found: {pipeline_id}")
        if self._yaml_generator is None:
            raise RuntimeError("YamlGeneratorPort is required to export YAML")
        yaml_content = self._yaml_generator.generate(pipeline)
        return {"pipeline_id": pipeline_id, "pipeline_yaml": yaml_content}
