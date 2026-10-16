from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DbtExecutionMetrics:
    exit_code: int
    models_passed: int
    tests_passed: int
    tests_failed: int
    elapsed_time: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "exit_code": self.exit_code,
            "models_passed": self.models_passed,
            "tests_passed": self.tests_passed,
            "tests_failed": self.tests_failed,
            "elapsed_time": self.elapsed_time,
        }


class RunResultsCollector:
    """Collects and normalizes dbt run_results.json into canonical platform metrics.

    Adheres strictly to Single Responsibility Principle (SRP) and Fail-Fast validation.
    """

    def collect(
        self, run_results_path: Path | str, target_metrics_path: Path | str | None = None
    ) -> DbtExecutionMetrics:
        path = Path(run_results_path)
        if not path.exists():
            raise FileNotFoundError(f"dbt run_results.json not found at expected path: {path}")

        try:
            raw_text = path.read_text(encoding="utf-8")
            data = json.loads(raw_text)
        except Exception as exc:
            raise ValueError(
                f"Corrupted or invalid JSON in run_results.json at {path}: {exc}"
            ) from exc

        if not isinstance(data, dict) or "results" not in data:
            raise ValueError(f"Invalid dbt run_results schema at {path}: missing 'results' field")

        results = data.get("results", [])
        models_passed = sum(1 for r in results if r.get("status") == "success")
        tests_passed = sum(1 for r in results if r.get("status") == "pass")
        tests_failed = sum(1 for r in results if r.get("status") in ("fail", "error"))
        elapsed_time = float(data.get("elapsed_time", 0.0))

        # Overall exit code is non-zero if any test or model failed/errored
        exit_code = 1 if tests_failed > 0 else 0

        metrics = DbtExecutionMetrics(
            exit_code=exit_code,
            models_passed=models_passed,
            tests_passed=tests_passed,
            tests_failed=tests_failed,
            elapsed_time=elapsed_time,
        )

        if target_metrics_path:
            t_path = Path(target_metrics_path)
            t_path.parent.mkdir(parents=True, exist_ok=True)
            t_path.write_text(json.dumps(metrics.to_dict(), indent=2), encoding="utf-8")

        return metrics
