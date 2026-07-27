"""
Guardrail Node — deterministic, non-LLM validation.

Enforces business rules the LLM cannot reliably self-police:
1. Compute sizing vs historical volume
2. PII governance (quality rules required when PII detected)
3. Staging bucket presence
4. Transform ref completeness

Returns FRESH validation_errors each run — does NOT accumulate.
"""

from __future__ import annotations

from typing import Any

from src.domain.schemas.pipeline_spec import PipelineSpec


def guardrail_node(state: dict[str, Any]) -> dict[str, Any]:
    """Runs deterministic validation. Returns state patch with errors and iteration count."""
    spec: PipelineSpec = state["pipeline_spec"]
    ctx: dict[str, Any] = state.get("context", {})
    errors: list[str] = []

    _check_compute_sizing(spec, ctx, errors)
    _check_pii_governance(spec, ctx, errors)
    _check_staging_bucket(spec, errors)
    _check_transform_ref(spec, errors)

    return {"validation_errors": errors, "iteration_count": state.get("iteration_count", 0) + 1}


def _check_compute_sizing(spec: PipelineSpec, ctx: dict[str, Any], errors: list[str]) -> None:
    gb: float = ctx.get("avg_volume_gb", 0.0)
    c = spec.compute
    if gb > 100.0:
        if c.engine not in ("spark", "dataflow"):
            errors.append(
                f"COMPUTE_SIZING: {gb:.1f} GB > 100 GB threshold — "
                f"engine must be 'spark' or 'dataflow', got '{c.engine}'."
            )
        if c.num_workers < 4:
            errors.append(
                f"COMPUTE_SIZING: {gb:.1f} GB > 100 GB — workers must be >= 4, got {c.num_workers}."
            )
    if 0.0 < gb < 10.0 and c.num_workers > 4:
        errors.append(
            f"COMPUTE_SIZING: {gb:.1f} GB < 10 GB — "
            f"num_workers={c.num_workers} is over-provisioned (max 4 for this volume)."
        )


def _check_pii_governance(spec: PipelineSpec, ctx: dict[str, Any], errors: list[str]) -> None:
    pii = ctx.get("pii_columns", [])
    if pii and not spec.quality.metrics:
        errors.append(
            f"GOVERNANCE: PII columns {pii!r} detected in context. "
            f"At least one quality rule is required to signal intentional governance."
        )


def _check_staging_bucket(spec: PipelineSpec, errors: list[str]) -> None:
    if not spec.compute.staging_bucket or not spec.compute.staging_bucket.strip():
        errors.append(
            "STAGING_BUCKET: compute.staging_bucket cannot be empty. "
            "Provide a valid gs:// or s3:// URI."
        )


def _check_transform_ref(spec: PipelineSpec, errors: list[str]) -> None:
    if spec.transform.engine != "none" and not spec.transform.ref:
        errors.append(
            f"TRANSFORM_REF: transform.engine='{spec.transform.engine}' "
            f"requires a non-empty transform.ref."
        )
