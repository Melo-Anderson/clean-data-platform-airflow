from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class ValidationRequest(BaseModel):
    pipeline_yaml: str = Field(description="Raw YAML content of the pipeline configuration.")
    pipeline_type: Literal["relational", "file", "api"] = Field(
        default="relational", description="Type of pipeline to apply specific validation rules."
    )


class ValidationErrorDetail(BaseModel):
    json_pointer: str = Field(
        description="JSON Pointer (RFC 6901) indicating the exact field with the error."
    )
    error_code: str = Field(description="Categorized error code (e.g., MISSING_ID, INVALID_SQL).")
    message: str = Field(description="Technical message describing the problem.")
    suggestion: str = Field(description="Direct and actionable corrective suggestion.")


class ValidationResponse(BaseModel):
    is_valid: bool = Field(description="True if the YAML meets all platform requirements.")
    errors: list[ValidationErrorDetail] = Field(
        default_factory=list, description="List of validation errors; empty if is_valid is True."
    )


class HarnessSchemaResponse(BaseModel):
    type: str = Field(description="The root type of the schema (usually 'object').")
    properties: dict[str, Any] = Field(description="The properties defined in the schema.")


class HarnessGoldExamplesResponse(BaseModel):
    examples: list[dict[str, Any]] = Field(description="List of canonical gold standard examples.")
