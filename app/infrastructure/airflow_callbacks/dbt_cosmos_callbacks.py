from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from app.infrastructure.adapters.dbt.cosmos.run_results import RunResultsCollector

logger = logging.getLogger(__name__)


def collect_dbt_execution_metrics(
    *,
    pipeline_id: str,
    project_dir: str | Path,
    output_base_dir: str | Path,
    target_rel_path: str = "target/run_results.json",
) -> dict[str, Any]:
    """Collects dbt run_results.json from the dbt target directory and saves canonical metrics.json.

    Adheres strictly to Fail-Fast principles without fallback heuristics.

    Returns:
        dict containing job_id, status, metrics_path, and output_path.
    """
    proj_path = Path(project_dir)
    results_path = proj_path / target_rel_path

    out_base = Path(output_base_dir) / pipeline_id
    out_base.mkdir(parents=True, exist_ok=True)
    metrics_path = out_base / "metrics.json"

    collector = RunResultsCollector()
    metrics = collector.collect(results_path, metrics_path)

    status_str = "success" if metrics.exit_code == 0 else "failed"

    return {
        "job_id": f"dbt-cosmos-{pipeline_id}",
        "status": status_str,
        "metrics_path": str(metrics_path),
        "schema_path": str(proj_path / "target" / "manifest.json"),
        "output_path": str(out_base),
    }
