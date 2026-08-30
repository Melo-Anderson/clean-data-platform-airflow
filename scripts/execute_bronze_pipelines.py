"""End-to-End User Execution Script: OTG_bronze Pipelines.

Simulates a real user interacting with the Data Platform exclusively
via HTTP REST APIs (Authentication, Endpoint/Asset Registration, Discovery,
Pipeline Registration, and Airflow Pipeline Triggering).
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import contextlib

# Configure UTF-8 encoding on Windows
if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    with contextlib.suppress(Exception):
        sys.stdout.reconfigure(encoding="utf-8")

import httpx
import jwt as pyjwt
from rich.console import Console
from rich.panel import Panel

console = Console(highlight=False)

# Fixed RSA Private Key for generating test JWTs
PRIVATE_KEY_PEM = """-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCp17PsSTf3e03m
wR76GCgm3zpASYab1XkGJirst/NZvQZ88A1u2QTiQeWhO7TDLXinko2n0ZFxNZSX
2/wQcBMKCnwWxq/xFE6b73zHQkoduj+YQj2f+8xvY+Iq0oEyIi6DKKFm27jsd+uY
CYauZnr9dKKbv7ruv+L0KgwosCxqrCsxNhDZl/08/lSb2LXfIybJuh6VMQBRLqkT
15pDIybwSGCjy4BgIyUEqwjOc+AcoYDMv0107TWMu4IaCvgiUPZihzZZsqAV090l
yiuyF53+rv84oLL+zHy/NG7Mpii7vJnTaUPf9bBFW7MLwjwdlkh4ov4/MSJqsITy
Y+oJG3adAgMBAAECggEABDMZt1N+J0fsvrJyxiNXxtJJOfK3ed327qB9+jl4MnVa
ljdHVcDW/pM7jtePmi3jKF2W1Bn5+y8ke/bMDkn/JoXo2JVUH2VtpixvTOwGMiL7
VJP6uxx6SxzQqFdpK2it9r9H8mendG1orWs64dAV5XN/W9OLV0D2Zyws/cqRZpfN
5aZyf1871UvHQgK49kjWQ69ipGZM92bc/vESGxpAZeKKYSYXtkkWxMzpAR7SeSZ5
zIQrd5cX94OzKhoGqAGQUTWTetfBTIsczRu0K+bDBwwE59nMtUQ3M5F5ic3fEQMR
WdF6cowUPB8yHFHsEVY3boA9VATO3EQxnDLENCzCrwKBgQDjj2/7e32EaH7HUkUv
p3hEeztKgf/1N7JvIlo5Sa11v50QKhwAicKYgaLfTmddtzXdrnt8cZQ+OGnR+qGn
90IaY1zcnYEHk6UTldN6h3v0aFQTUzMG2OcAgJsV66hzxg1DyMpnG1Fa5XAmRZll
1rbOMJz2Ck9B5LU3ZkRvygXjDwKBgQC/EaUzfZVED7i7DgW+xY/IjZVJzQ8tvkfz
1TOYtmvlxkg4v8CVLvQ/b+N2qqaZn3wTH9mAU0YUOM4Q1dfvPrD4d+A63Rg32+1U
tEwc46/5PMaCtGxmO7WLccFgk1wyaTkc30h8jofuqJmaR0y3HVv/0M29meLsR+N3
0q3AFMCbkwKBgQDDGvJKTiDZ67X3M4R6TT4CiR3WzgsktjJYsr1krNT6ReVmPJRx
qaucklmQ2Goroa+fd8AMfF0706Z3EEqV9ptIgLTXunssgdxhJG6DebI/ZUvgnc78
KfA1MA7IBpsRWFd7LKbNLFDefCVhyv6woB1wP6H0GfbGak8tRpOavT265QKBgGj1
Z3umk/WEcWUH6e4HFtoDtKuK4ritG1d9mc9c/l6Fkqzh4QfSeEfUze4lBknDi2Py
DgfpNsjq/3/OCMWa+Zo0N8/+HkypGnF6bYk9JjDSyvWH6Tgruqm0Ppcvu+jRVpde
rLIHlfJrWZ2fZyv8C8q2SB7MRxSm1PTAncOzYq7TAoGBAODoOW0Knt4TdFh3cdbF
GFWEULjJG5Y5AasIKRn8QpjCOaKVwib78gJZtj9DalUFiJ6pYsTd4YibB5/2XVLm
UHROCgh5z7TbPnCEobz5nLv0Z3ZGuAZJiUD4mNNAKhtLE0BXpzSQBy9wl2a56HCZ
nqPPnQGKt6gwFDkPJwzkr4lY
-----END PRIVATE KEY-----"""


def _get_token(role: str) -> str:
    """Generate RS256 JWT bearer token for the specified platform role."""
    payload = {
        "sub": f"user_{role}",
        "email": f"{role}@company.com",
        "roles": [role],
        "exp": int(time.time()) + 86400,
    }
    return pyjwt.encode(payload, PRIVATE_KEY_PEM, algorithm="RS256")


async def get_http_client() -> httpx.AsyncClient:
    """Returns an AsyncClient configured to communicate with the Platform REST API."""
    api_url = os.getenv("API_URL", "http://127.0.0.1:8000")
    client = httpx.AsyncClient(base_url=api_url, timeout=120.0)
    try:
        res = await client.get("/health")
        if res.status_code == 200:
            console.print(f"[cyan]Connected to Data Platform API at: {api_url}[/cyan]")
            return client
    except Exception:
        pass

    fallback_url = "http://localhost:8000"
    console.print(f"[yellow]Retrying connection at: {fallback_url}[/yellow]")
    return httpx.AsyncClient(base_url=fallback_url, timeout=120.0)


async def main() -> None:
    console.print(
        Panel.fit(
            "[bold cyan]END-TO-END REST API EXECUTION: PLATFORM_BRONZE[/bold cyan]\n"
            "[white]Simulating user execution via Data Platform REST APIs[/white]",
            border_style="cyan",
        )
    )

    token_sre = _get_token("sre")
    token_ae = _get_token("analytics_engineer")

    headers_sre = {"Authorization": f"Bearer {token_sre}", "Content-Type": "application/json"}
    headers_ae = {"Authorization": f"Bearer {token_ae}", "Content-Type": "application/json"}

    client = await get_http_client()

    try:
        # =========================================================================
        # STEP 1: [SRE] Register Storage Endpoint
        # =========================================================================
        console.print(
            "\n[bold yellow]STEP 1: [SRE] Registering Storage Endpoint via REST API...[/bold yellow]"
        )
        endpoint_payload = {
            "name": "platform-landing-storage",
            "credential_ref": "vault/none",
            "root_path": "/opt/airflow/data/landing",
            "technical_description": "Landing zone storage for platform bronze files",
        }
        res = await client.post(
            "/v1/endpoints/file_system", json=endpoint_payload, headers=headers_sre
        )
        if res.status_code in (200, 201):
            console.print(f"[green][OK] Endpoint registered: {res.json().get('name')}[/green]")
        elif res.status_code == 409 or "already exists" in res.text:
            console.print("[yellow][INFO] Endpoint already exists. Proceeding.[/yellow]")
        else:
            console.print(f"[red]Failed registering endpoint ({res.status_code}): {res.text}[/red]")

        # =========================================================================
        # STEP 2: [Analytics Engineer] Register & Activate Data Asset 'platform_bronze'
        # =========================================================================
        console.print(
            "\n[bold yellow]STEP 2: [Analytics Engineer] Registering Data Asset 'platform_bronze'...[/bold yellow]"
        )
        asset_payload = {
            "name": "platform_bronze",
            "description": "Platform Bronze Tier Raw Ingestion Dataset",
            "owner_email": "ae_gaming@company.com",
            "tags": ["gaming", "bronze", "raw"],
            "policy_tags": ["PUBLIC"],
            "discovery_schedule": "0 * * * *",
            "discovery_scope_include": ["*.csv", "*.json"],
            "discovery_scope_exclude": ["*.tmp"],
        }
        res = await client.post("/v1/assets/", json=asset_payload, headers=headers_ae)
        if res.status_code in (200, 201):
            console.print(f"[green][OK] Asset registered: {res.json().get('name')}[/green]")
        else:
            console.print(f"[yellow][INFO] Asset response ({res.status_code}): {res.text}[/yellow]")

        console.print(
            "\n[bold yellow]STEP 3: [SRE] Activating Data Asset with Storage Endpoint...[/bold yellow]"
        )
        res = await client.post(
            "/v1/assets/platform_bronze/activate?endpoint_name=platform-landing-storage",
            headers=headers_sre,
        )
        if res.status_code in (200, 201):
            console.print("[green][OK] Asset activated successfully.[/green]")
        else:
            console.print(
                f"[yellow][INFO] Asset activation status ({res.status_code}): {res.text}[/yellow]"
            )

        # =========================================================================
        # STEP 4: [Analytics Engineer] Trigger Metadata Discovery Scan
        # =========================================================================
        console.print(
            "\n[bold yellow]STEP 4: [Analytics Engineer] Triggering Metadata Discovery Scan...[/bold yellow]"
        )
        disc_payload = {
            "triggered_by": "user_api_execution",
        }
        res = await client.post(
            "/v1/discovery/assets/platform_bronze/run", json=disc_payload, headers=headers_ae
        )
        if res.status_code in (200, 201):
            disc_data = res.json()
            console.print(
                f"[green][OK] Discovery run triggered: id={disc_data.get('id')} | status={disc_data.get('status')}[/green]"
            )
        else:
            console.print(
                f"[yellow][INFO] Discovery scan response ({res.status_code}): {res.text}[/yellow]"
            )

        # =========================================================================
        # STEP 5: [Analytics Engineer] Register & Trigger Ingestion Pipelines via API
        # =========================================================================
        pipelines_to_run = [
            {
                "name": "Ingest_affiliate_cpa_ftd_Platform_Bronze",
                "object_name": "affiliate_cpa_ftd",
                "quality_rules": [{"type": "not_null", "column": "affiliate_id"}],
            },
            {
                "name": "Ingest_players_Platform_Bronze",
                "object_name": "players",
                "quality_rules": [{"type": "not_null", "column": "player_id"}],
            },
            {
                "name": "Ingest_sessions_Platform_Bronze",
                "object_name": "sessions",
                "quality_rules": [{"type": "not_null", "column": "session_id"}],
            },
            {
                "name": "Ingest_transactions_Platform_Bronze",
                "object_name": "transactions",
                "quality_rules": [{"type": "not_null", "column": "transaction_id"}],
            },
        ]

        console.print(
            "\n[bold yellow]STEP 5: [Analytics Engineer] Registering and Triggering Ingestion Pipelines via API...[/bold yellow]"
        )

        triggered_runs: list[dict] = []

        for pipe_spec in pipelines_to_run:
            pipe_name = pipe_spec["name"]
            obj_name = pipe_spec["object_name"]

            pipe_payload = {
                "name": pipe_name,
                "pipeline_type": "ingestion",
                "owner_email": "ae_gaming@company.com",
                "source_asset": "platform_bronze",
                "destination_asset": "platform_bronze",
                "cron_schedule": "0 * * * *",
                "destination_objects": [{"object_name": obj_name, "create_if_not_exists": True}],
                "source_objects": [
                    {
                        "object_id": f"asset-platform-bronze.{obj_name}",
                        "load_strategy": "incremental",
                    }
                ],
                "compute": {
                    "engine": "omnibeam",
                    "num_workers": 1,
                    "machine_type": "n1-standard-2",
                },
                "quality_rules": pipe_spec["quality_rules"],
                "airflow_config": {
                    "retries": 1,
                    "retry_delay_minutes": 1,
                    "execution_timeout_minutes": 60,
                    "sla_minutes": 90,
                },
            }

            # 1. Register Pipeline via API
            res = await client.post("/v1/pipelines/", json=pipe_payload, headers=headers_ae)
            pipeline_id = ""
            if res.status_code in (200, 201):
                pipeline_id = res.json()["id"]
                console.print(
                    f"[green][OK] Pipeline registered: {pipe_name} (ID: {pipeline_id})[/green]"
                )
            else:
                # Retrieve existing pipeline
                res_all = await client.get("/v1/pipelines/", headers=headers_ae)
                if res_all.status_code == 200:
                    for p in res_all.json():
                        if p.get("name") == pipe_name:
                            pipeline_id = p.get("id")
                            break
                if not pipeline_id:
                    pipeline_id = f"pipe_{obj_name}"
                console.print(
                    f"[yellow][INFO] Using existing pipeline: {pipe_name} (ID: {pipeline_id})[/yellow]"
                )

            # 2. Trigger Pipeline Run via API
            console.print(f"  -> Triggering Airflow DAG run for: [cyan]{pipe_name}[/cyan]...")
            trigger_payload = {"triggered_by": "user_api_execution"}
            res_trig = await client.post(
                f"/v1/pipelines/{pipeline_id}/run", json=trigger_payload, headers=headers_ae
            )
            if res_trig.status_code in (200, 201):
                run_data = res_trig.json()
                console.print(
                    f"    [green][OK] Airflow DAG triggered: dag_run_id={run_data.get('dag_run_id')} (status={run_data.get('status')})[/green]"
                )
                triggered_runs.append(
                    {
                        "pipeline_id": pipeline_id,
                        "pipeline_name": pipe_name,
                        "run_id": run_data.get("id"),
                        "dag_run_id": run_data.get("dag_run_id"),
                    }
                )
            else:
                console.print(
                    f"    [yellow][INFO] Trigger response ({res_trig.status_code}): {res_trig.text}[/yellow]"
                )

        # =========================================================================
        # STEP 6: Monitor Execution Status via Platform API
        # =========================================================================
        console.print(
            "\n[bold yellow]STEP 6: Monitoring Pipeline Runs in Platform Database...[/bold yellow]"
        )
        for item in triggered_runs:
            p_id = item["pipeline_id"]
            p_name = item["pipeline_name"]
            console.print(f"  -> Polling run status for [cyan]{p_name}[/cyan]...")
            for attempt in range(25):
                await asyncio.sleep(4)
                res_status = await client.get(
                    f"/v1/pipelines/{p_id}/runs/latest", headers=headers_ae
                )
                if res_status.status_code == 200:
                    status_info = res_status.json()
                    current_status = status_info.get("status")
                    if current_status in ("success", "failed"):
                        console.print(
                            f"     Final Status: [bold green]{current_status.upper()}[/bold green] | Finished: {status_info.get('finished_at')}"
                        )
                        break
                    elif attempt % 3 == 0:
                        console.print(
                            f"     Status: [cyan]{current_status}[/cyan] (in progress...)"
                        )

    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
