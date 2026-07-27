"""
File-based MetricsPort adapter. Reads {base_path}/{object_id}/metrics.json.
Expected format:
{"object_id": "...", "runs": [{"status": "...", "volume_gb": N, "duration_seconds": N}]}
"""

from __future__ import annotations

import json
import statistics
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ExecutionMetricsSummary:
    object_id: str
    avg_volume_gb: float
    avg_duration_seconds: float
    p95_duration_seconds: float
    last_run_status: str
    sample_size: int


class StorageMetricsReader:
    def __init__(self, base_path: str) -> None:
        self._base = Path(base_path)

    def get_execution_metrics(self, object_id: str) -> ExecutionMetricsSummary | None:
        f = self._base / object_id / "metrics.json"
        if not f.exists():
            return None
        data = json.loads(f.read_text())
        runs = data.get("runs", [])
        if not runs:
            return None
        vols = [r["volume_gb"] for r in runs if "volume_gb" in r]
        durs = [r["duration_seconds"] for r in runs if "duration_seconds" in r]
        return ExecutionMetricsSummary(
            object_id=object_id,
            avg_volume_gb=statistics.mean(vols) if vols else 0.0,
            avg_duration_seconds=statistics.mean(durs) if durs else 0.0,
            p95_duration_seconds=(sorted(durs)[int(len(durs) * 0.95)] if len(durs) > 1 else 0.0),
            last_run_status=runs[-1].get("status", "unknown"),
            sample_size=len(runs),
        )
