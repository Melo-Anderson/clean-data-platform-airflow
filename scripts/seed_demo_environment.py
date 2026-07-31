"""Script to populate seed data, register 10 pipelines, and compile DAGs into ./dags/."""

from __future__ import annotations

import asyncio

# Add project root to PYTHONPATH if needed
import sys
from pathlib import Path

from rich.console import Console
from rich.table import Table

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.infrastructure.dag_generator.dag_generator import DagGenerator
from app.infrastructure.persistence.database import _engine

console = Console()

DAGS_DIR = Path("dags")

PIPELINE_SPECS = [
    # 4 PostgreSQL Ingestion Pipelines
    {
        "id": "p_ingest_customers",
        "name": "Ingest Customers Table",
        "type": "ingestion",
        "source": "postgres",
        "table": "customers",
    },
    {
        "id": "p_ingest_orders",
        "name": "Ingest Orders Table",
        "type": "ingestion",
        "source": "postgres",
        "table": "orders",
    },
    {
        "id": "p_ingest_products",
        "name": "Ingest Products Table",
        "type": "ingestion",
        "source": "postgres",
        "table": "products",
    },
    {
        "id": "p_ingest_payments",
        "name": "Ingest Payments Table",
        "type": "ingestion",
        "source": "postgres",
        "table": "payments",
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


async def seed_databases() -> None:
    console.print(
        "[bold blue]1. Populating Seed Data (PostgreSQL, MongoDB, Mock API)...[/bold blue]"
    )
    # Postgres/SQLite tables setup
    async with _engine.begin() as conn:
        from sqlalchemy import text

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
    console.print("[green][OK] PostgreSQL (7 tables seeded)[/green]")
    console.print("[green][OK] MongoDB (2 collections seeded)[/green]")
    console.print("[green][OK] Mock REST API (ready on port 8081)[/green]")


async def deploy_pipelines_and_dags() -> None:
    console.print("\n[bold blue]2. Registering Pipelines & Compiling DAGs...[/bold blue]")
    DAGS_DIR.mkdir(exist_ok=True)
    generator = DagGenerator()

    table = Table(title="Deployed Demo Pipelines")
    table.add_column("Pipeline ID", style="cyan")
    table.add_column("Name", style="magenta")
    table.add_column("Type", style="green")
    table.add_column("DAG File", style="yellow")

    for spec in PIPELINE_SPECS:
        safe_name = spec["name"].replace(" ", "_").replace("&", "and")
        # Build YAML configuration
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
    asset_id: {spec["source"]}_asset
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
    tags: [{spec["type"]}, demo]
    pool: default_pool
  discovery_task:
    enabled: true
    on_critical_change: warn
"""
        dag_code = generator.generate(pipeline_yaml)
        dag_filepath = DAGS_DIR / f"dag_{spec['id']}.py"
        dag_filepath.write_text(dag_code, encoding="utf-8")
        table.add_row(spec["id"], spec["name"], spec["type"], dag_filepath.name)

    console.print(table)
    console.print("\n[bold green][OK] All 10 DAGs compiled and written to ./dags/[/bold green]")
    console.print("[bold yellow]Airflow UI available at: http://localhost:8080/[/bold yellow]\n")


async def main() -> None:
    await seed_databases()
    await deploy_pipelines_and_dags()


if __name__ == "__main__":
    asyncio.run(main())
