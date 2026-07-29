from __future__ import annotations

import sqlglot
import yaml
from sqlglot.errors import ParseError

from app.domain.pipelines.validation import ValidationError, ValidationResult
from app.infrastructure.dag_generator.dag_generator import DagGenerator


class PipelineValidator:
    """
    Unified validation engine for pipeline YAMLs.
    Executes 3-stage validation:
    1. YAML Structure & Required Fields
    2. SQL AST Validation (via sqlglot)
    3. Dry-run DAG compilation (via DagGenerator)
    """

    def __init__(self, dag_generator: DagGenerator | None = None) -> None:
        self._dag_generator = dag_generator or DagGenerator()

    def validate(self, pipeline_yaml: str, pipeline_type: str = "relational") -> ValidationResult:
        errors: list[ValidationError] = []

        try:
            data = yaml.safe_load(pipeline_yaml)
        except Exception as exc:
            return ValidationResult(
                is_valid=False,
                errors=[
                    ValidationError(
                        json_pointer="/",
                        error_code="YAML_PARSE_ERROR",
                        message=str(exc),
                        suggestion="Check the YAML syntax and indentation.",
                    )
                ],
            )

        if not data or not isinstance(data, dict):
            return ValidationResult(
                is_valid=False,
                errors=[
                    ValidationError(
                        json_pointer="/",
                        error_code="EMPTY_OR_INVALID_YAML",
                        message="YAML root must be a non-empty dictionary.",
                        suggestion="Ensure the YAML file contains valid key-value pairs.",
                    )
                ],
            )

        pipeline_config = data.get("pipeline", data)
        if "pipeline_id" not in pipeline_config:
            errors.append(
                ValidationError(
                    json_pointer="/pipeline_id",
                    error_code="MISSING_ID",
                    message="Field 'pipeline_id' is required.",
                    suggestion="Add 'pipeline_id' to the root of the YAML document.",
                )
            )

        source_query = pipeline_config.get("source_query")
        if source_query:
            try:
                sqlglot.parse_one(source_query)
            except ParseError as exc:
                errors.append(
                    ValidationError(
                        json_pointer="/source_query",
                        error_code="INVALID_SQL",
                        message=f"SQL syntax error: {exc}",
                        suggestion="Fix the SQL query syntax near the specified location.",
                    )
                )

        if errors:
            return ValidationResult(is_valid=False, errors=errors)

        try:
            self._dag_generator.generate(pipeline_yaml)
        except Exception as exc:
            errors.append(
                ValidationError(
                    json_pointer="/",
                    error_code="DAG_COMPILATION_ERROR",
                    message=f"Jinja2 DAG compilation failed: {exc}",
                    suggestion="Ensure all required template variables exist in the YAML.",
                )
            )
            return ValidationResult(is_valid=False, errors=errors)

        return ValidationResult(is_valid=True, errors=[])
