"""Register OTG Transformation Pipeline in Data Platform.

Registers the Transformation Pipeline with dbt compute engine,
reactive Airflow 3 Asset scheduling on 'platform://asset/OTG_bronze',
and outlets pointing to 'OTG_silver' and 'OTG_gold'.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx
import jwt as pyjwt
from rich.console import Console
from rich.panel import Panel

console = Console(highlight=False)

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
    payload = {
        "sub": f"user_{role}",
        "email": f"{role}@company.com",
        "roles": [role],
        "exp": int(time.time()) + 86400,
    }
    return pyjwt.encode(payload, PRIVATE_KEY_PEM, algorithm="RS256")


async def get_http_client() -> httpx.AsyncClient:
    api_url = os.getenv("API_URL", "http://127.0.0.1:8000")
    try:
        async with httpx.AsyncClient(base_url=api_url, timeout=5.0) as live_client:
            res = await live_client.get("/health")
            if res.status_code == 200:
                return httpx.AsyncClient(base_url=api_url, timeout=30.0)
    except Exception:
        pass
    from app.main import create_app

    app = create_app()
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://platform-api", timeout=60.0
    )


async def main() -> None:
    console.print(
        Panel.fit(
            "[bold cyan]REGISTER PLATFORM TRANSFORMATION PIPELINE (dbt)[/bold cyan]\n"
            "[white]Configuring reactive Airflow 3 Asset pipeline for Silver & Gold[/white]",
            border_style="cyan",
        )
    )

    client = await get_http_client()
    headers = {
        "Authorization": f"Bearer {_get_token('analytics_engineer')}",
        "Content-Type": "application/json",
    }

    try:
        # Register Destination Assets if not already registered
        for asset_name in ["platform_silver", "platform_gold"]:
            res = await client.post(
                "/v1/assets/",
                headers=headers,
                json={
                    "name": asset_name,
                    "description": f"Transformed {asset_name} dataset",
                    "owner_email": "analytics@company.com",
                    "discovery_schedule": "0 4 * * *",
                    "discovery_scope": {"include": []},
                },
            )
            if res.status_code in (200, 201):
                console.print(f"[green][OK] Registered Asset: {asset_name}[/green]")

        # Register Transformation Pipelines (Silver & Gold)
        pipelines_to_register = [
            {
                "name": "Platform_Silver_ETL",
                "pipeline_type": "transformation",
                "owner_email": "analytics@company.com",
                "source_asset": "platform_bronze",
                "destination_asset": "platform_silver",
                "source_objects": [],
                "destination_objects": [
                    {"object_name": "slv_players", "create_if_not_exists": True},
                    {"object_name": "slv_sessions", "create_if_not_exists": True},
                    {"object_name": "slv_transactions", "create_if_not_exists": True},
                    {"object_name": "slv_affiliate_cpa_ftd", "create_if_not_exists": True},
                ],
                "compute": {
                    "engine": "dbt",
                    "select": "staging silver",
                    "staging_bucket": "/opt/airflow/logs/dbt_outputs",
                    "num_workers": 1,
                    "machine_type": "n1-standard-2",
                },
                "quality_rules": [{"type": "not_null"}],
                "airflow_config": {
                    "retries": 1,
                    "retry_delay_minutes": 1,
                    "execution_timeout_minutes": 60,
                    "sla_minutes": 90,
                },
            },
            {
                "name": "Platform_Gold_Analytics",
                "pipeline_type": "transformation",
                "owner_email": "analytics@company.com",
                "source_asset": "platform_silver",
                "destination_asset": "platform_gold",
                "source_objects": [],
                "destination_objects": [
                    {"object_name": "dim_players", "create_if_not_exists": True},
                    {"object_name": "dim_affiliates", "create_if_not_exists": True},
                    {"object_name": "fct_transactions", "create_if_not_exists": True},
                    {"object_name": "fct_affiliate_performance", "create_if_not_exists": True},
                    {"object_name": "fct_player_risk_profile", "create_if_not_exists": True},
                    {"object_name": "gold_fraud_alerts", "create_if_not_exists": True},
                ],
                "compute": {
                    "engine": "dbt",
                    "select": "gold",
                    "staging_bucket": "/opt/airflow/logs/dbt_outputs",
                    "num_workers": 1,
                    "machine_type": "n1-standard-2",
                },
                "quality_rules": [{"type": "not_null"}],
                "airflow_config": {
                    "retries": 1,
                    "retry_delay_minutes": 1,
                    "execution_timeout_minutes": 60,
                    "sla_minutes": 90,
                },
            },
        ]

        registered_ids = []
        for pipe_payload in pipelines_to_register:
            pipe_name = pipe_payload["name"]
            res_pipe = await client.post("/v1/pipelines/", headers=headers, json=pipe_payload)
            pipeline_id = ""
            if res_pipe.status_code in (200, 201):
                pipe_data = res_pipe.json()
                pipeline_id = pipe_data.get("id")
                console.print(
                    f"[bold green][SUCCESS] Pipeline Registered: {pipe_name} (ID: {pipeline_id})[/bold green]"
                )
            else:
                console.print(
                    f"[yellow][INFO] Pipeline {pipe_name} ({res_pipe.status_code}): {res_pipe.text}[/yellow]"
                )
                res_all = await client.get("/v1/pipelines/", headers=headers)
                if res_all.status_code == 200:
                    for p in res_all.json():
                        if p.get("name") == pipe_name:
                            pipeline_id = p.get("id")
                            break
            if pipeline_id:
                registered_ids.append((pipe_name, pipeline_id))

        # Trigger Silver Pipeline (which produces OTG_silver and will automatically trigger Gold!)
        if registered_ids:
            first_name, first_id = registered_ids[0]
            console.print(
                f"\n[bold yellow]Triggering Airflow DAG run for {first_name} (ID: {first_id})...[/bold yellow]"
            )
            trigger_res = await client.post(
                f"/v1/pipelines/{first_id}/run",
                json={"triggered_by": "user_api_execution"},
                headers=headers,
            )
            if trigger_res.status_code in (200, 201):
                run_info = trigger_res.json()
                run_id = run_info.get("id")
                console.print(
                    f"[green][OK] Airflow DAG triggered: dag_run_id={run_info.get('dag_run_id')} (status={run_info.get('status')})[/green]"
                )
                console.print(f"[cyan]Monitoring {first_name} execution...[/cyan]")
                while True:
                    await asyncio.sleep(5)
                    try:
                        poll_res = await client.get(f"/v1/pipelines/runs/{run_id}", headers=headers)
                        if poll_res.status_code == 200:
                            status = poll_res.json().get("status", "running")
                            console.print(f"   Status: {status}")
                            if status in ("success", "partial", "failed", "quality_failed"):
                                console.print(
                                    f"[bold green]Final Status for {first_name}: {status.upper()}[/bold green]"
                                )
                                break
                    except Exception as poll_exc:
                        console.print(f"   [yellow]Waiting for status ({poll_exc})...[/yellow]")
            else:
                console.print(
                    f"[yellow][INFO] Trigger response ({trigger_res.status_code}): {trigger_res.text}[/yellow]"
                )

    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
