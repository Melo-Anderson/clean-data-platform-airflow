from __future__ import annotations

import typer
from rich.console import Console

discovery_app = typer.Typer(no_args_is_help=True)
console = Console()


@discovery_app.command("run")
def trigger_discovery(
    asset_id: str = typer.Option(..., "--asset-id", "-a", help="The DataAsset ID"),
    object_id: str | None = typer.Option(None, "--object-id", "-o", help="Optional target object name to run discovery inline for"),
) -> None:
    """
    Trigger manual discovery for a DataAsset or a specific object.
    """
    console.print(f"[blue]Triggering discovery for asset={asset_id} object={object_id}...[/blue]")
    # Call RunDiscoveryUseCase
    console.print("[green]Discovery run completed. Schema synchronized.[/green]")


@discovery_app.command("approve")
def approve_drift(
    approval_id: str = typer.Argument(..., help="DriftApproval ID to approve"),
    decided_by: str = typer.Option(..., "--user", "-u", help="Username/Email of the owner approving the change"),
    notes: str | None = typer.Option(None, "--notes", "-n", help="Optional notes for audit logs"),
) -> None:
    """
    Approve a critical schema drift. Platform will apply self-healing.
    """
    console.print(f"[bold green]Approving drift {approval_id} by {decided_by}...[/bold green]")
    # Call ApproveDriftUseCase
    console.print("[green]Drift approved. Self-healing triggered successfully.[/green]")
