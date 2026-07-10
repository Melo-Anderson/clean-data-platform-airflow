from __future__ import annotations

import typer

from cli.commands.discovery_commands import discovery_app
from cli.commands.pipeline_commands import pipeline_app

app = typer.Typer(
    name="platform",
    help="Platform Command Line Interface for AE and SRE engineers.",
    no_args_is_help=True,
    rich_markup_mode="markdown",
)

# Registra os sub-aplicativos
app.add_typer(pipeline_app, name="pipeline", help="Manage and rebuild Airflow Pipelines.")
app.add_typer(
    discovery_app, name="discovery", help="Run metadata discovery and manage schema drifts."
)

if __name__ == "__main__":
    app()
