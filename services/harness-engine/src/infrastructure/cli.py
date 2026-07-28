"""CLI: python -m src.infrastructure.cli generate "..." --save-to pipeline.yaml"""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.syntax import Syntax

from src.application.graph.state import initial_state
from src.application.graph.workflow import build_graph
from src.config import settings
from src.infrastructure.adapters.db_schema_reader import DbSchemaReader
from src.infrastructure.adapters.http_platform_reader import HttpPlatformReader
from src.infrastructure.adapters.storage_metrics_reader import StorageMetricsReader
from src.infrastructure.llm_factory import get_llm

app = typer.Typer(name="harness-engine")
console = Console()


@app.command()
def generate(
    prompt: str = typer.Argument(..., help="Natural language pipeline description."),
    save_to: Path | None = typer.Option(None, "--save-to", help="Save YAML output to this file."),
) -> None:
    """Generate a validated pipeline YAML from a natural language prompt."""
    platform_reader = HttpPlatformReader(
        schema_url=settings.platform_schema_url,
        examples_url=settings.platform_examples_url,
    )
    graph = build_graph(
        metadata_port=DbSchemaReader(settings.platform_db_url),  # type: ignore[arg-type]
        metrics_port=StorageMetricsReader(settings.metrics_storage_path),
        schema_port=platform_reader,
        examples_port=platform_reader,
        llm=get_llm(),
    )
    console.print(Panel(f"[bold cyan]Prompt:[/] {prompt}", title="Harness Engine AI"))
    with Progress(
        SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console
    ) as p:
        t = p.add_task("[cyan]Running LangGraph...", total=None)
        result = graph.invoke(initial_state(prompt))
        p.remove_task(t)

    status = result.get("status", "unknown")
    yaml_out = result.get("generated_yaml", "")
    errors = result.get("validation_errors", [])

    st_color = "[green]approved[/]" if status == "approved" else f"[red]{status}[/]"
    console.print(f"\n[bold]Status:[/] {st_color}")
    console.print(f"[bold]Iterations:[/] {result.get('iteration_count', 0)}")
    for e in errors:
        console.print(f"  [red]• {e}[/]")
    if yaml_out:
        console.print(Syntax(yaml_out, "yaml", theme="monokai", line_numbers=True))
        if save_to:
            save_to.write_text(yaml_out, encoding="utf-8")
            console.print(f"[green]Saved to {save_to}[/]")


if __name__ == "__main__":
    app()
