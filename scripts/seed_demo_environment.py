"""Script to populate demo environment seed data, register discovery metadata, and compile DAGs.

Follows Clean Code principles (§3.2, §5.1, §6.1, §7) and platform business rules.
Integrates the complete Discovery lifecycle:
  1. Relational schema (demo) & OpenBao secrets seeding
  2. Endpoints registration (Database, NoSQL, REST API)
  3. DataAssets registration & activation
  4. Triggering metadata discovery runs & snapshot verification
  5. Compiling and deploying 10 demo DAGs into ./dags/
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import jwt as pyjwt
from rich.console import Console
from rich.table import Table
from sqlalchemy import text

# Add project root to PYTHONPATH
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.infrastructure.dag_generator.dag_generator import DagGenerator
from app.infrastructure.persistence.database import get_engine

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


def _generate_auth_token(role: str) -> str:
    """Generate RS256 JWT for role-based API access."""
    payload = {
        "sub": f"u1_seed_{role}",
        "email": f"{role}_seed@company.com",
        "roles": [role],
        "exp": int(time.time()) + 3600,
    }
    return pyjwt.encode(payload, PRIVATE_KEY_PEM, algorithm="RS256")


# Standard Demo Pipeline Specifications
PIPELINE_SPECS = [
    # 4 PostgreSQL Ingestion Pipelines (schema demo)
    {
        "id": "p_ingest_customers",
        "name": "Ingest Customers Table",
        "type": "ingestion",
        "source": "postgres",
        "table": "demo.customers",
    },
    {
        "id": "p_ingest_orders",
        "name": "Ingest Orders Table",
        "type": "ingestion",
        "source": "postgres",
        "table": "demo.orders",
    },
    {
        "id": "p_ingest_products",
        "name": "Ingest Products Table",
        "type": "ingestion",
        "source": "postgres",
        "table": "demo.products",
    },
    {
        "id": "p_ingest_payments",
        "name": "Ingest Payments Table",
        "type": "ingestion",
        "source": "postgres",
        "table": "demo.payments",
    },
    # 2 MongoDB Ingestion Pipelines
    {
        "id": "p_ingest_user_events",
        "name": "Ingest User Events Collection",
        "type": "ingestion",
        "source": "mongodb",
        "table": "user_events",
    },
    {
        "id": "p_ingest_clickstream",
        "name": "Ingest Clickstream Collection",
        "type": "ingestion",
        "source": "mongodb",
        "table": "clickstream",
    },
    # 1 REST API Ingestion Pipeline
    {
        "id": "p_ingest_mock_api",
        "name": "Ingest Store Transactions API",
        "type": "ingestion",
        "source": "rest_api",
        "table": "transactions",
    },
    # 2 ETL Pipelines
    {
        "id": "p_etl_sales_analytics",
        "name": "ETL Sales & Customer Analytics",
        "type": "etl",
        "source": "dwh",
        "table": "sales_summary",
    },
    {
        "id": "p_etl_user_behavior",
        "name": "ETL User Behavior Aggregations",
        "type": "etl",
        "source": "dwh",
        "table": "behavior_summary",
    },
    # 1 Export Pipeline
    {
        "id": "p_export_financial_report",
        "name": "Export Financial Report to SFTP",
        "type": "export",
        "source": "dwh",
        "table": "financial_report",
    },
]


@dataclass(frozen=True)
class EndpointSpec:
    url_path: str
    name: str
    body: dict[str, Any]


@dataclass(frozen=True)
class DataAssetSpec:
    name: str
    description: str
    owner_email: str
    tags: list[str]
    discovery_schedule: str
    discovery_scope_include: list[str]
    discovery_scope_exclude: list[str]
    endpoint_name: str


class DemoDatabaseSeeder:
    """Handles PostgreSQL demo schema DDL/DML and OpenBao vault credentials."""

    @staticmethod
    async def seed_relational_schema() -> None:
        console.print("[bold blue]1. Populating PostgreSQL Demo Schema & Records...[/bold blue]")
        async with get_engine().begin() as conn:
            queries = [
                "CREATE SCHEMA IF NOT EXISTS demo;",
                "CREATE TABLE IF NOT EXISTS demo.customers (id INTEGER PRIMARY KEY, name VARCHAR(100), email VARCHAR(100));",
                "CREATE TABLE IF NOT EXISTS demo.orders (id INTEGER PRIMARY KEY, customer_id INT, amount DECIMAL(10,2));",
                "CREATE TABLE IF NOT EXISTS demo.order_items (id INTEGER PRIMARY KEY, order_id INT, product_id INT);",
                "CREATE TABLE IF NOT EXISTS demo.products (id INTEGER PRIMARY KEY, title VARCHAR(100), price DECIMAL(10,2));",
                "CREATE TABLE IF NOT EXISTS demo.categories (id INTEGER PRIMARY KEY, name VARCHAR(100));",
                "CREATE TABLE IF NOT EXISTS demo.payments (id INTEGER PRIMARY KEY, order_id INT, status VARCHAR(20));",
                "CREATE TABLE IF NOT EXISTS demo.inventory (id INTEGER PRIMARY KEY, product_id INT, stock INT);",
                "CREATE TABLE IF NOT EXISTS demo.transactions (id INTEGER PRIMARY KEY, customer_id INT, product_id INT, amount DECIMAL(10,2));",
                "INSERT INTO demo.customers (id, name, email) VALUES (1, 'Alice Smith', 'alice@corp.com'), (2, 'Bob Jones', 'bob@corp.com') ON CONFLICT DO NOTHING;",
                "INSERT INTO demo.products (id, title, price) VALUES (1, 'Laptop', 1200.00), (2, 'Mouse', 25.50) ON CONFLICT DO NOTHING;",
                "INSERT INTO demo.orders (id, customer_id, amount) VALUES (101, 1, 1225.50), (102, 2, 50.00) ON CONFLICT DO NOTHING;",
                "INSERT INTO demo.payments (id, order_id, status) VALUES (1, 101, 'COMPLETED'), (2, 102, 'PENDING') ON CONFLICT DO NOTHING;",
            ]
            for q in queries:
                await conn.execute(text(q))
        console.print(
            "[green][OK] PostgreSQL schema 'demo' created with all target tables & seed rows[/green]"
        )

    @staticmethod
    def seed_nosql_schema() -> None:
        console.print(
            "[bold blue]1.1. Populating MongoDB Demo Collections & Records...[/bold blue]"
        )
        mongo_host = "mongodb" if _in_docker else os.getenv("MONGO_HOST", "localhost")
        mongo_port = int(os.getenv("MONGO_PORT", "27017"))
        try:
            from pymongo import MongoClient

            client = MongoClient(
                host=mongo_host,
                port=mongo_port,
                username="admin",
                password="password",
                authSource="admin",
                serverSelectionTimeoutMS=3000,
            )
            db = client["test_db"]
            user_events = db["user_events"]
            if user_events.count_documents({}) == 0:
                user_events.insert_many(
                    [
                        {"id": "evt_1", "user_id": 101, "event_type": "page_view", "url": "/home"},
                        {
                            "id": "evt_2",
                            "user_id": 102,
                            "event_type": "add_to_cart",
                            "url": "/cart",
                        },
                        {
                            "id": "evt_3",
                            "user_id": 103,
                            "event_type": "checkout",
                            "url": "/checkout",
                        },
                    ]
                )
            clickstream = db["clickstream"]
            if clickstream.count_documents({}) == 0:
                clickstream.insert_many(
                    [
                        {"id": "clk_1", "session_id": "s_1001", "url": "/home", "duration_sec": 45},
                        {
                            "id": "clk_2",
                            "session_id": "s_1002",
                            "url": "/products",
                            "duration_sec": 120,
                        },
                    ]
                )
            client.close()
            console.print(
                "[green][OK] MongoDB collections 'user_events' and 'clickstream' seeded successfully[/green]"
            )
        except Exception as exc:
            console.print(f"  [yellow][INFO][/yellow] MongoDB direct connection skipped ({exc})")

    @staticmethod
    async def seed_vault_secrets() -> None:
        """Ensure OpenBao has valid connection parameters including schema: demo."""
        bao_url = os.getenv("PLATFORM_VAULT_URL", "http://openbao:8200")
        if not _in_docker and "localhost" not in bao_url and "127.0.0.1" not in bao_url:
            bao_url = "http://127.0.0.1:8200"

        headers = {"X-Vault-Token": "root"}
        secrets_to_seed = [
            (
                "secret/data/postgres",
                {
                    "driver": "postgres",
                    "user": "airflow",
                    "password": "airflow",
                    "host": os.getenv("POSTGRES_HOST", "postgres"),
                    "port": 5432,
                    "database": "platform_db",
                    "schema": "demo",
                    "sslmode": "disable",
                },
            ),
            (
                "secret/data/mongo",
                {
                    "driver": "mongodb",
                    "user": "admin",
                    "password": "password",
                    "host": "mongodb",
                    "port": 27017,
                    "database": "test_db",
                    "auth_source": "admin",
                },
            ),
            (
                "secret/data/mock-store",
                {
                    "token": "e2e-test-token",
                    "base_url": f"http://{_mock_api_host}:8081",
                    "auth_type": "bearer",
                },
            ),
        ]

        try:
            async with httpx.AsyncClient(timeout=4.0) as client:
                for path, payload in secrets_to_seed:
                    resp = await client.post(
                        f"{bao_url}/v1/{path}", json={"data": payload}, headers=headers
                    )
                    if resp.status_code in (200, 204):
                        console.print(f"  [green][OK][/green] OpenBao secret configured: {path}")
        except Exception as exc:
            console.print(
                f"  [yellow][INFO][/yellow] OpenBao not accessible directly ({exc}); skipping secret seeding"
            )


class PlatformDiscoveryRegistrar:
    """Manages Endpoints, DataAssets, and Discovery orchestration via Platform API."""

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        self._sre_token = _generate_auth_token("sre")
        self._ae_token = _generate_auth_token("analytics_engineer")

    @property
    def headers_sre(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._sre_token}"}

    @property
    def headers_ae(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._ae_token}"}

    async def is_api_available(self) -> bool:
        """Check if Platform API is online."""
        try:
            async with httpx.AsyncClient(base_url=self.base_url, timeout=3.0) as client:
                resp = await client.get("/docs")
                return resp.status_code == 200
        except Exception:
            return False

    async def register_endpoints(self, client: httpx.AsyncClient) -> None:
        console.print("\n[yellow]2.1. Registering Platform Endpoints (SRE Role)...[/yellow]")
        endpoints = [
            EndpointSpec(
                url_path="/v1/endpoints/database",
                name="e2e-db-prod",
                body={
                    "name": "e2e-db-prod",
                    "credential_ref": "secret/postgres",
                    "technical_description": "PostgreSQL Production Database Endpoint",
                },
            ),
            EndpointSpec(
                url_path="/v1/endpoints/nosql",
                name="e2e-mongo-prod",
                body={
                    "name": "e2e-mongo-prod",
                    "credential_ref": "secret/mongo",
                    "technical_description": "MongoDB Production NoSQL Endpoint",
                },
            ),
            EndpointSpec(
                url_path="/v1/endpoints/rest_api",
                name="e2e-api-store-mock-prod",
                body={
                    "name": "e2e-api-store-mock-prod",
                    "credential_ref": "secret/mock-store",
                    "base_url": f"http://{_mock_api_host}:8081",
                    "auth_type": "bearer",
                    "technical_description": "Store Transactions Mock REST API Endpoint",
                },
            ),
        ]

        for ep in endpoints:
            try:
                resp = await client.post(ep.url_path, json=ep.body, headers=self.headers_sre)
                if resp.status_code in (201, 409):
                    console.print(f"  [green][OK][/green] Endpoint registered: {ep.name}")
                else:
                    console.print(
                        f"  [red][WARN {resp.status_code}][/red] Endpoint {ep.name}: {resp.text}"
                    )
            except Exception as err:
                console.print(f"  [red][ERROR][/red] Failed registering endpoint {ep.name}: {err}")

    async def register_data_assets(self, client: httpx.AsyncClient) -> list[DataAssetSpec]:
        console.print("\n[yellow]2.2. Registering DataAssets (Analytics Engineer Role)...[/yellow]")
        assets = [
            DataAssetSpec(
                name="postgres_asset",
                description="PostgreSQL Core Business Data Asset (schema demo)",
                owner_email="data-team@company.com",
                tags=["postgres", "relational", "demo"],
                discovery_schedule="0 0 * * *",
                discovery_scope_include=["demo.*", "demo_*"],
                discovery_scope_exclude=[],
                endpoint_name="e2e-db-prod",
            ),
            DataAssetSpec(
                name="mongodb_asset",
                description="MongoDB Clickstream & User Events Data Asset",
                owner_email="data-team@company.com",
                tags=["mongo", "nosql", "demo"],
                discovery_schedule="0 0 * * *",
                discovery_scope_include=["*"],
                discovery_scope_exclude=[],
                endpoint_name="e2e-mongo-prod",
            ),
            DataAssetSpec(
                name="rest_api_asset",
                description="REST API Store Transactions Data Asset",
                owner_email="data-team@company.com",
                tags=["api", "transactions", "demo"],
                discovery_schedule="0 0 * * *",
                discovery_scope_include=["*Product*", "*Customer*", "*orders*", "*"],
                discovery_scope_exclude=[],
                endpoint_name="e2e-api-store-mock-prod",
            ),
        ]

        for asset in assets:
            payload = {
                "name": asset.name,
                "description": asset.description,
                "owner_email": asset.owner_email,
                "tags": asset.tags,
                "policy_tags": [],
                "discovery_schedule": asset.discovery_schedule,
                "discovery_scope_include": asset.discovery_scope_include,
                "discovery_scope_exclude": asset.discovery_scope_exclude,
            }
            try:
                resp = await client.post("/v1/assets/", json=payload, headers=self.headers_ae)
                if resp.status_code in (201, 409):
                    console.print(f"  [green][OK][/green] DataAsset registered: {asset.name}")
                else:
                    console.print(
                        f"  [red][WARN {resp.status_code}][/red] Asset {asset.name}: {resp.text}"
                    )
            except Exception as err:
                console.print(f"  [red][ERROR][/red] Failed registering asset {asset.name}: {err}")

        return assets

    async def activate_data_assets(
        self, client: httpx.AsyncClient, assets: list[DataAssetSpec]
    ) -> None:
        console.print("\n[yellow]2.3. Activating DataAssets (SRE Role)...[/yellow]")
        for asset in assets:
            try:
                resp = await client.post(
                    f"/v1/assets/{asset.name}/activate",
                    params={"endpoint_name": asset.endpoint_name},
                    headers=self.headers_sre,
                )
                if resp.status_code in (200, 422):
                    console.print(
                        f"  [green][OK][/green] Asset activated: {asset.name} -> {asset.endpoint_name}"
                    )
                else:
                    console.print(
                        f"  [red][WARN {resp.status_code}][/red] Activate {asset.name}: {resp.text}"
                    )
            except Exception as err:
                console.print(f"  [red][ERROR][/red] Failed activating asset {asset.name}: {err}")

    async def trigger_discovery_runs(
        self, client: httpx.AsyncClient, assets: list[DataAssetSpec]
    ) -> None:
        console.print("\n[yellow]2.4. Triggering Discovery Runs...[/yellow]")
        for asset in assets:
            try:
                resp = await client.post(
                    f"/v1/discovery/assets/{asset.name}/run",
                    json={"triggered_by": "seed_demo_script"},
                    headers=self.headers_ae,
                )
                if resp.status_code in (200, 201):
                    run_id = resp.json().get("id", "N/A")
                    console.print(
                        f"  [green][OK][/green] Discovery started for {asset.name} (Run ID: {run_id})"
                    )
                else:
                    console.print(
                        f"  [red][WARN {resp.status_code}][/red] Discovery {asset.name}: {resp.text}"
                    )
            except Exception as err:
                console.print(
                    f"  [red][ERROR][/red] Discovery trigger error for {asset.name}: {err}"
                )

    async def verify_discovery_snapshots(
        self, client: httpx.AsyncClient, assets: list[DataAssetSpec]
    ) -> None:
        console.print("\n[yellow]2.5. Verifying Discovered Schema Snapshots...[/yellow]")
        table = Table(title="Discovered Schema Snapshots")
        table.add_column("Asset Name", style="cyan")
        table.add_column("Discovered Objects", style="magenta")
        table.add_column("Total Fields", style="green")

        for asset in assets:
            try:
                resp = await client.get(
                    f"/v1/discovery/assets/{asset.name}/snapshot",
                    headers=self.headers_ae,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    objects = list(data.get("objects", {}).keys())
                    fields_count = len(data.get("fields", []))
                    obj_str = ", ".join(objects[:5]) + (
                        f" (+{len(objects) - 5} more)" if len(objects) > 5 else ""
                    )
                    table.add_row(asset.name, obj_str or "None", str(fields_count))
                else:
                    table.add_row(asset.name, f"[yellow]Status {resp.status_code}[/yellow]", "0")
            except Exception as err:
                table.add_row(asset.name, f"[red]Error: {err}[/red]", "0")

        console.print(table)

    async def execute_discovery_lifecycle(self) -> None:
        available = await self.is_api_available()
        if not available:
            console.print(
                f"[yellow][NOTICE] Platform API ({self.base_url}) is offline. "
                "Skipping HTTP discovery steps. DAG generation will proceed.[/yellow]"
            )
            return

        async with httpx.AsyncClient(base_url=self.base_url, timeout=30.0) as client:
            await self.register_endpoints(client)
            assets = await self.register_data_assets(client)
            await self.activate_data_assets(client, assets)
            await self.trigger_discovery_runs(client, assets)
            # Brief delay to allow auto-provisioning to persist
            await asyncio.sleep(2)
            await self.verify_discovery_snapshots(client, assets)


class DemoDagCompiler:
    """Compiles demo pipeline YAMLs into Airflow DAG scripts."""

    @staticmethod
    def compile_pipeline_yaml(spec: dict[str, Any]) -> str:
        safe_name = spec["name"].replace(" ", "_").replace("&", "and")
        clean_table = spec["table"].split(".")[-1]
        compute_engine = (
            "omnibeam"
            if spec["type"] == "ingestion"
            else ("dbt" if spec["type"] == "etl" else "none")
        )
        staging_bucket = (
            "/opt/airflow/logs/omnibeam_outputs" if spec["type"] == "ingestion" else "/tmp/staging"
        )
        credential_ref = (
            "secret/postgres"
            if spec["source"] == "postgres"
            else "secret/mongo"
            if spec["source"] == "mongodb"
            else "secret/mock-store"
            if spec["source"] == "rest_api"
            else f"secret/{spec['source']}"
        )
        source_type = (
            "database"
            if spec["source"] == "postgres"
            else "mongodb"
            if spec["source"] == "mongodb"
            else "rest_api"
            if spec["source"] == "rest_api"
            else "storage"
        )
        driver_line = (
            "      driver: postgres\n"
            if spec["source"] == "postgres"
            else "      driver: mongodb\n"
            if spec["source"] == "mongodb"
            else "      driver: rest_api\n"
            if spec["source"] == "rest_api"
            else ""
        )
        endpoint_line = (
            f"      endpoint: /api/v1/{clean_table}\n" if spec["source"] == "rest_api" else ""
        )

        source_asset = "dwh_lakehouse" if spec["source"] == "dwh" else f"{spec['source']}_asset"
        dest_asset = "dwh_lakehouse" if spec["source"] == "dwh" else f"{spec['source']}_asset"

        return f"""
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
    asset_name: {source_asset}
    objects:
      - object_name: {spec["table"]}
        load_strategy: incremental
        page_size: 1000
  destination:
    asset_name: {dest_asset}
    objects:
      - object_name: {clean_table}
        create_if_not_exists: true
  transform:
    engine: {"dbt" if spec["type"] == "etl" else "none"}
    ref: {"marts/" + clean_table if spec["type"] == "etl" else ""}
  compute:
    engine: {compute_engine}
    staging_bucket: {staging_bucket}
    num_workers: 2
    config:
      source_type: {source_type}
      credential_ref: {credential_ref}
{driver_line}{endpoint_line}      num_workers: 2
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
    tags: [{spec["type"]}, demo]
    pool: default_pool
  discovery_task:
    enabled: true
    on_critical_change: warn
"""

    @classmethod
    def deploy(cls, target_dir: Path = DAGS_DIR) -> list[Path]:
        target_dir.mkdir(parents=True, exist_ok=True)
        generator = DagGenerator()
        created_paths: list[Path] = []

        table = Table(title="Compiled Demo DAGs")
        table.add_column("Pipeline ID", style="cyan")
        table.add_column("Name", style="magenta")
        table.add_column("Type", style="green")
        table.add_column("Target DAG File", style="yellow")

        for spec in PIPELINE_SPECS:
            pipeline_yaml = cls.compile_pipeline_yaml(spec)
            dag_code = generator.generate(pipeline_yaml)
            dag_filepath = target_dir / f"dag_{spec['id']}.py"
            dag_filepath.write_text(dag_code, encoding="utf-8")
            created_paths.append(dag_filepath)
            table.add_row(spec["id"], spec["name"], spec["type"], dag_filepath.name)

        console.print(table)
        console.print(
            f"\n[bold green][OK] Successfully compiled {len(created_paths)} DAGs in {target_dir}/[/bold green]"
        )
        return created_paths


def generate_dags_only() -> None:
    """Callable entry point used by test suites and lightweight local runners."""
    DemoDagCompiler.deploy(DAGS_DIR)


async def main() -> None:
    # 1. Relational schema, NoSQL schema & secret seeding
    await DemoDatabaseSeeder.seed_relational_schema()
    DemoDatabaseSeeder.seed_nosql_schema()
    await DemoDatabaseSeeder.seed_vault_secrets()

    # 2. Platform Discovery Lifecycle (Endpoints, Assets, Activation, Discovery)
    registrar = PlatformDiscoveryRegistrar(base_url=API_URL)
    await registrar.execute_discovery_lifecycle()

    # 3. Compiling and deploying DAGs
    console.print("\n[bold blue]3. Compiling DAGs to ./dags/...[/bold blue]")
    DemoDagCompiler.deploy(DAGS_DIR)
    console.print("[bold yellow]Airflow UI available at: http://localhost:8080/[/bold yellow]\n")


if __name__ == "__main__":
    asyncio.run(main())
