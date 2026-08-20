from __future__ import annotations

from typing import Protocol, runtime_checkable

from app.domain.pipelines.pipeline import Pipeline


@runtime_checkable
class YamlGeneratorPort(Protocol):
    """Port for generating YAML representations of Domain Pipelines."""

    def generate(self, pipeline: Pipeline) -> str: ...


@runtime_checkable
class DagGeneratorPort(Protocol):
    """Port for compiling pipeline YAML definitions into Airflow DAG Python code."""

    def generate(self, pipeline_yaml: str) -> str: ...
