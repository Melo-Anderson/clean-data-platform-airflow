from __future__ import annotations

_GOLD_EXAMPLES: dict[str, str] = {
    "ingestion": (
        "schema_version: '1.0'\n"
        "pipeline_id: p_ingest_sales\n"
        "name: Ingest Sales Daily\n"
        "type: ingestion\n"
        "owner: eng@company.com\n"
        "schedule:\n"
        "  mode: cron\n"
        "  cron: '0 6 * * *'\n"
    ),
    "etl": (
        "schema_version: '1.0'\n"
        "pipeline_id: p_etl_sales_daily\n"
        "name: ETL Sales Daily Aggregation\n"
        "type: etl\n"
        "owner: analytics@company.com\n"
        "schedule:\n"
        "  mode: trigger_with_gate\n"
        "  cron: '0 8 * * *'\n"
    ),
    "export": (
        "schema_version: '1.0'\n"
        "pipeline_id: p_export_sales_report\n"
        "name: Export Sales Report to Partner\n"
        "type: export\n"
        "owner: data-ops@company.com\n"
        "schedule:\n"
        "  mode: trigger\n"
    ),
}


class GetHarnessGoldExamplesUseCase:
    async def execute(self, pipeline_type: str = "all") -> dict[str, str]:
        return _GOLD_EXAMPLES
