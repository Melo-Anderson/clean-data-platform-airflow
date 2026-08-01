"""Script to seed platform data, register endpoints, assets, discovery runs, 10 pipelines, and compile DAGs via HTTP API.

Follows platform business rules (docs/business_rules.md).
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

import httpx
import jwt as pyjwt
from rich.console import Console
from rich.table import Table
from sqlalchemy import text

# Add project root to PYTHONPATH
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.infrastructure.dag_generator.dag_generator import DagGenerator
from app.infrastructure.persistence.database import _engine

console = Console()
DAGS_DIR = Path("dags")

_in_docker = os.path.exists("/.dockerenv") or os.getenv("API_URL", "").startswith(
    "http://platform-api"
)
_api_host = "platform-api" if _in_docker else "127.0.0.1"
_mock_api_host = os.getenv("MOCK_API_HOST", "mock-api")
API_URL = os.getenv("API_URL", f"http://{_api_host}:8000")

# Private key for JWT generation (matching tests/e2e/conftest.py)
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
        "sub": f"u1_seed_{role}",
        "email": f"{role}_seed@company.com",
        "roles": [role],
        "exp": int(time.time()) + 3600,
    }
    return pyjwt.encode(payload, PRIVATE_KEY_PEM, algorithm="RS256")


PIPELINE_SPECS = [
    # 7 PostgreSQL Ingestion Pipelines
    {"id": "p_ingest_customers", "name": "Ingest Customers Table", "type": "ingestion", "source": "postgres", "table": "demo_customers"},
    {"id": "p_ingest_orders", "name": "Ingest Orders Table", "type": "ingestion", "source": "postgres", "table": "demo_orders"},
    {"id": "p_ingest_products", "name": "Ingest Products Table", "type": "ingestion", "source": "postgres", "table": "demo_products"},
    {"id": "p_ingest_payments", "name": "Ingest Payments Table", "type": "ingestion", "source": "postgres", "table": "demo_payments"},
    {"id": "p_ingest_categories", "name": "Ingest Categories Table", "type": "ingestion", "source": "postgres", "table": "demo_categories"},
    {"id": "p_ingest_order_items", "name": "Ingest Order Items Table", "type": "ingestion", "source": "postgres", "table": "demo_order_items"},
    {"id": "p_ingest_inventory", "name": "Ingest Inventory Table", "type": "ingestion", "source": "postgres", "table": "demo_inventory"},
    # 2 MongoDB Ingestion Pipelines
    {"id": "p_ingest_user_events", "name": "Ingest User Events Collection", "type": "ingestion", "source": "mongodb", "table": "user_events"},
    {"id": "p_ingest_clickstream", "name": "Ingest Clickstream Collection", "type": "ingestion", "source": "mongodb", "table": "clickstream"},
    # 1 REST API Ingestion Pipeline
    {"id": "p_ingest_mock_api", "name": "Ingest Store Transactions API", "type": "ingestion", "source": "rest_api", "table": "transactions"},
]


async def seed_local_databases() -> None:
    console.print(
        "[bold blue]1. Populating Local Databases (PostgreSQL/SQLite)...[/bold blue]"
    )
    async with _engine.begin() as conn:
        queries = [
            "CREATE TABLE IF NOT EXISTS demo_customers (id INTEGER PRIMARY KEY, name VARCHAR(100), email VARCHAR(100));",
            "CREATE TABLE IF NOT EXISTS demo_orders (id INTEGER PRIMARY KEY, customer_id INT, amount DECIMAL(10,2));",
            "CREATE TABLE IF NOT EXISTS demo_order_items (id INTEGER PRIMARY KEY, order_id INT, product_id INT);",
            "CREATE TABLE IF NOT EXISTS demo_products (id INTEGER PRIMARY KEY, title VARCHAR(100), price DECIMAL(10,2));",
            "CREATE TABLE IF NOT EXISTS demo_categories (id INTEGER PRIMARY KEY, name VARCHAR(100));",
            "CREATE TABLE IF NOT EXISTS demo_payments (id INTEGER PRIMARY KEY, order_id INT, status VARCHAR(20));",
            "CREATE TABLE IF NOT EXISTS demo_inventory (id INTEGER PRIMARY KEY, product_id INT, stock INT);",
        ]
        for q in queries:
            await conn.execute(text(q))
    console.print("[green][OK] Local PostgreSQL/SQLite (7 demo_* tables created)[/green]")


async def run_platform_e2e_seed() -> None:
    console.print(f"\n[bold blue]2. Running Platform Business Flow via HTTP API ({API_URL})...[/bold blue]")
    sre_token = _get_token("sre")
    ae_token = _get_token("analytics_engineer")

    headers_sre = {"Authorization": f"Bearer {sre_token}"}
    headers_ae = {"Authorization": f"Bearer {ae_token}"}

    async with httpx.AsyncClient(base_url=API_URL, timeout=30.0) as client:
        # Step A: Register Endpoints (SRE)
        console.print("[yellow]A1. Registering Endpoints (SRE Role)...[/yellow]")

        endpoints = [
            {
                "url": "/v1/endpoints/database",
                "body": {
                    "name": "e2e-db-prod",
                    "credential_ref": "secret/postgres",
                    "technical_description": "PostgreSQL Production Database Endpoint",
                },
            },
            {
                "url": "/v1/endpoints/nosql",
                "body": {
                    "name": "e2e-mongo-prod",
                    "credential_ref": "secret/mongo",
                    "technical_description": "MongoDB Production Database Endpoint",
                },
            },
            {
                "url": "/v1/endpoints/rest_api",
                "body": {
                    "name": "e2e-api-store-mock-prod",
                    "credential_ref": "secret/mock-store",
                    "base_url": f"http://{_mock_api_host}:8081",
                    "auth_type": "bearer",
                    "technical_description": "Store Transactions Mock REST API Endpoint",
                },
            },
        ]

        for ep in endpoints:
            try:
                resp = await client.post(ep["url"], json=ep["body"], headers=headers_sre)
                if resp.status_code in (201, 409):
                    console.print(f"  [green][OK][/green] Endpoint registered: {ep['body']['name']}")
                else:
                    console.print(f"  [bold red][ERROR {resp.status_code}][/bold red] Endpoint {ep['body']['name']} failed: {resp.text}")
            except Exception as err:
                console.print(f"  [bold red][REQUEST FAILED][/bold red] Endpoint {ep['body']['name']}: {err}")

        # Step B: Register DataAssets (Analytics Engineer)
        console.print("\n[yellow]A2. Registering DataAssets (Analytics Engineer Role)...[/yellow]")
        assets = [
            {
                "name": "e2e-postgres-asset",
                "description": "PostgreSQL Core Business Data Asset",
                "owner_email": "data-team@company.com",
                "tags": ["postgres", "relational"],
                "policy_tags": [],
                "discovery_schedule": "0 0 * * *",
                "discovery_scope_include": ["demo_*"],  # Scope to demo_* to exclude internal platform_db tables
                "discovery_scope_exclude": [],
                "endpoint": "e2e-db-prod",
            },
            {
                "name": "e2e-mongo-asset",
                "description": "MongoDB Clickstream & User Events Data Asset",
                "owner_email": "data-team@company.com",
                "tags": ["mongo", "nosql"],
                "policy_tags": [],
                "discovery_schedule": "0 0 * * *",
                "discovery_scope_include": ["*"],
                "discovery_scope_exclude": [],
                "endpoint": "e2e-mongo-prod",
            },
            {
                "name": "e2e-api-store-mock-asset",
                "description": "REST API Store Transactions Data Asset",
                "owner_email": "data-team@company.com",
                "tags": ["api", "transactions"],
                "policy_tags": [],
                "discovery_schedule": "0 0 * * *",
                "discovery_scope_include": ["*"],
                "discovery_scope_exclude": [],
                "endpoint": "e2e-api-store-mock-prod",
            },
        ]

        asset_ids: dict[str, str] = {}
        for asset in assets:
            payload = {
                "name": asset["name"],
                "description": asset["description"],
                "owner_email": asset["owner_email"],
                "tags": asset["tags"],
                "policy_tags": asset["policy_tags"],
                "discovery_schedule": asset["discovery_schedule"],
                "discovery_scope_include": asset["discovery_scope_include"],
                "discovery_scope_exclude": asset["discovery_scope_exclude"],
            }
            try:
                resp = await client.post("/v1/assets/", json=payload, headers=headers_ae)
                if resp.status_code == 201:
                    asset_ids[asset["name"]] = resp.json()["id"]
                    console.print(f"  [green][OK][/green] Asset created: {asset['name']}")
                elif resp.status_code == 409:
                    resp_get = await client.get(f"/v1/assets/{asset['name']}", headers=headers_ae)
                    if resp_get.status_code == 200:
                        asset_ids[asset["name"]] = resp_get.json()["id"]
                        console.print(f"  [green][OK][/green] Asset already exists: {asset['name']}")
                    else:
                        console.print(
                            f"  [bold red][ERROR][/bold red] Asset {asset['name']} conflict but GET failed: {resp_get.text}"
                        )
                else:
                    console.print(
                        f"  [bold red][ERROR {resp.status_code}][/bold red] Asset {asset['name']} failed: {resp.text}"
                    )
            except Exception as err:
                console.print(f"  [bold red][REQUEST FAILED][/bold red] Asset {asset['name']}: {err}")

        # Step C: Activate DataAssets (SRE Role)
        console.print("\n[yellow]A3. Activating DataAssets (DRAFT -> ACTIVE via SRE Role)...[/yellow]")
        for asset in assets:
            try:
                resp = await client.post(
                    f"/v1/assets/{asset['name']}/activate",
                    params={"endpoint_name": asset["endpoint"]},
                    headers=headers_sre,
                )
                if resp.status_code in (200, 422):
                    console.print(f"  [green][OK][/green] Asset activated: {asset['name']}")
                else:
                    console.print(f"  [bold red][ERROR {resp.status_code}][/bold red] Activate {asset['name']} failed: {resp.text}")
            except Exception as err:
                console.print(f"  [bold red][REQUEST FAILED][/bold red] Activate {asset['name']}: {err}")

        # Step D: Trigger Metadata Discovery (Analytics Engineer Role)
        console.print("\n[yellow]A4. Triggering Metadata Discovery...[/yellow]")
        for asset in assets:
            try:
                resp = await client.post(
                    f"/v1/discovery/assets/{asset['name']}/run",
                    json={"triggered_by": "seed_e2e_script"},
                    headers=headers_ae,
                )
                if resp.status_code in (201, 200):
                    data = resp.json()
                    run_id = data.get("id", "N/A")
                    console.print(f"  [green][OK][/green] Discovery triggered for {asset['name']} (Run ID: {run_id})")
                else:
                    console.print(f"  [bold red][ERROR {resp.status_code}][/bold red] Discovery {asset['name']} failed: {resp.text}")
            except Exception as err:
                console.print(f"  [bold red][REQUEST FAILED][/bold red] Discovery {asset['name']}: {err}")

        # Step E: Register 10 Pipelines & Compile DAGs
        console.print("\n[yellow]3. Registering 10 Pipelines & Compiling DAGs...[/yellow]")
        DAGS_DIR.mkdir(parents=True, exist_ok=True)
        generator = DagGenerator()

        table = Table(title="E2E Registered Pipelines")
        table.add_column("Pipeline ID", style="cyan")
        table.add_column("Name", style="magenta")
        table.add_column("Source Asset", style="blue")
        table.add_column("DAG File", style="yellow")

        for spec in PIPELINE_SPECS:
            if spec["source"] == "postgres":
                asset_key = "e2e-postgres-asset"
            elif spec["source"] == "mongodb":
                asset_key = "e2e-mongo-asset"
            else:
                asset_key = "e2e-api-store-mock-asset"

            source_asset_id = asset_ids.get(asset_key, asset_key)
            safe_name = spec["name"].replace(" ", "_").replace("&", "and")

            pipeline_payload = {
                "name": safe_name,
                "pipeline_type": spec["type"],
                "owner_email": "demo@company.com",
                "source_asset_id": source_asset_id,
                "cron_schedule": "0 * * * *",
                "destination_asset_id": source_asset_id,
                "destination_objects": [
                    {
                        "name": f"{spec['table']}_stg",
                        "create_if_not_exists": True,
                    }
                ],
            }
            try:
                resp_pipe = await client.post("/v1/pipelines/", json=pipeline_payload, headers=headers_ae)
                if resp_pipe.status_code in (201, 422):
                    console.print(f"  [green][OK][/green] Pipeline registered via API: {safe_name}")
                else:
                    console.print(f"  [bold red][ERROR {resp_pipe.status_code}][/bold red] Pipeline {safe_name} failed: {resp_pipe.text}")
            except Exception as err:
                console.print(f"  [bold red][REQUEST FAILED][/bold red] Pipeline {safe_name}: {err}")

            pipeline_yaml = f"""
schema_version: '1.0'
pipeline:
  id: {spec["id"]}
  name: {safe_name}
  type: {spec["type"]}
  owner: demo@company.com
  schedule:
    mode: cron
    cron: "0 * * * *"
  source:
    asset_id: {source_asset_id}
    objects:
      - object_id: {spec["table"]}
        load_strategy: incremental
        page_size: 1000
  destination:
    asset_id: dwh_lakehouse
    objects:
      - object_id: {spec["table"]}_stg
        create_if_not_exists: true
  transform:
    engine: {"dbt" if spec["type"] == "etl" else "none"}
    ref: {"marts/" + spec["table"] if spec["type"] == "etl" else ""}
  compute:
    engine: {"rest_api" if "api" in spec["source"] else "duckdb"}
    staging_bucket: /tmp/staging
    num_workers: 2
    config:
      credential_ref: {spec["source"]}_db_credentials
      source_table: {spec["table"]}
      num_workers: 2
      memory: "4G"
  quality:
    metrics:
      - name: "not_null"
        column: "id"
  airflow:
    retries: 2
    retry_delay_minutes: 5
    execution_timeout_minutes: 60
    sla_minutes: 30
    tags: [{spec["type"]}, e2e]
    pool: default_pool
  discovery_task:
    enabled: true
    on_critical_change: warn
"""
            dag_code = generator.generate(pipeline_yaml)
            dag_filepath = DAGS_DIR / f"dag_{spec['id']}.py"
            dag_filepath.write_text(dag_code, encoding="utf-8")
            table.add_row(spec["id"], spec["name"], asset_key, dag_filepath.name)

        console.print(table)
        console.print("\n[bold green][OK] Platform E2E Seed Finished![/bold green]")
        console.print("[bold yellow]Airflow UI available at: http://localhost:8080/[/bold yellow]\n")


async def main() -> None:
    await seed_local_databases()
    await run_platform_e2e_seed()


if __name__ == "__main__":
    asyncio.run(main())
