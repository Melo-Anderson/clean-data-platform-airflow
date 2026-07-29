"""
Generator Node (Node 2) — LLM Structured Output Generation.

On iteration_count > 0, prepends format_feedback_prompt() to the human message.
LLM injected via make_generator_node() factory for easy test mocking.
"""

from __future__ import annotations

from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from src.domain.schemas.pipeline_spec import PipelineSpec
from src.domain.schemas.yaml_exporter import dump, format_feedback_prompt

_SYSTEM_TEMPLATE = """\
You are a precise Data Platform YAML Specification Generator.
Generate a complete, valid PipelineSpec JSON for the requested pipeline.

{platform_rules}

## Pipeline Context:
{context_summary}

## Reference Examples of Correct YAML Structure:
{few_shot_examples}

Follow the platform rules exactly. Do not add unknown fields. Do not omit required fields.
"""


def make_generator_node(llm: ChatOpenAI | None = None) -> Any:
    """Factory: returns generator_node closed over the injected LLM."""
    if llm is None:
        from src.config import settings

        _llm: ChatOpenAI = ChatOpenAI(
            model=settings.openai_model,
            temperature=settings.openai_temperature,
            api_key=settings.openai_api_key,
        )
    else:
        _llm = llm
    structured = _llm.with_structured_output(PipelineSpec)

    def generator_node(state: dict[str, Any]) -> dict[str, Any]:
        ctx: dict[str, Any] = state.get("context", {})
        errors: list[str] = state.get("validation_errors", [])
        iteration: int = state.get("iteration_count", 0)

        few_shot = "\n---\n".join(
            f"# {ex['description']}\n{ex['yaml_snippet']}"
            for ex in ctx.get("few_shot_examples", [])
        )
        system = _SYSTEM_TEMPLATE.format(
            platform_rules=ctx.get("platform_rules", ""),
            context_summary=_summarize_context(ctx),
            few_shot_examples=few_shot,
        )
        human = state["user_prompt"]
        if iteration > 0 and errors:
            human = (
                f"{format_feedback_prompt(errors=errors, iteration=iteration)}\n\n---\n"
                f"Original request: {human}"
            )

        msgs = [SystemMessage(content=system), HumanMessage(content=human)]
        spec: PipelineSpec = structured.invoke(msgs)
        return {"messages": msgs, "pipeline_spec": spec, "generated_yaml": dump(spec)}

    return generator_node


def _summarize_context(ctx: dict[str, Any]) -> str:
    lines = []
    if ctx.get("avg_volume_gb"):
        lines.append(f"- Avg volume: {ctx['avg_volume_gb']:.2f} GB")
    if ctx.get("avg_duration_seconds"):
        lines.append(f"- Avg duration: {ctx['avg_duration_seconds']:.0f}s")
    if ctx.get("pii_columns"):
        lines.append(f"- PII columns detected: {ctx['pii_columns']}")
    return "\n".join(lines) if lines else "No historical context available."
