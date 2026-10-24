from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from app.domain.pipelines.watermark import Watermark, WatermarkType
from app.infrastructure.adapters.compute.omnibeam_compute_adapter import OmniBeamComputeAdapter
from app.infrastructure.adapters.compute.rest_api_helpers import calculate_parquet_metrics
from app.infrastructure.airflow_callbacks.compute_job_adapter import JobStatus


def test_calculate_parquet_metrics_with_integers(tmp_path: Path) -> None:
    parquet_path = tmp_path / "data.parquet"
    # Create pyarrow table with numbers 9 and 10. Lexicographically "9" > "10", but numerically 10 > 9.
    table = pa.Table.from_pydict({"id": [9, 10, 5], "name": ["a", "b", "c"]})
    pq.write_table(table, parquet_path)

    metrics = calculate_parquet_metrics(
        parquet_path,
        watermark_column="id",
        watermark_type=WatermarkType.INTEGER,
    )
    assert metrics["row_count"] == 3
    assert metrics["watermark"] == {
        "column": "id",
        "type": "integer",
        "value": "10",
    }


def test_calculate_parquet_metrics_with_timestamps(tmp_path: Path) -> None:
    parquet_path = tmp_path / "timestamps.parquet"
    table = pa.Table.from_pydict(
        {
            "ts": [
                "2026-10-24T08:00:00+00:00",
                "2026-10-24T12:00:00+00:00",
                "2026-10-24T10:00:00+00:00",
            ]
        }
    )
    pq.write_table(table, parquet_path)

    metrics = calculate_parquet_metrics(
        parquet_path,
        watermark_column="ts",
        watermark_type=WatermarkType.TIMESTAMP,
    )
    assert metrics["watermark"] == {
        "column": "ts",
        "type": "timestamp",
        "value": "2026-10-24T12:00:00+00:00",
    }


def test_calculate_parquet_metrics_zero_rows_preserves_incoming_watermark(tmp_path: Path) -> None:
    parquet_path = tmp_path / "empty.parquet"
    table = pa.Table.from_arrays(
        [pa.array([], type=pa.int64()), pa.array([], type=pa.string())],
        names=["id", "name"],
    )
    pq.write_table(table, parquet_path)

    incoming = Watermark.parse("100", WatermarkType.INTEGER)
    metrics = calculate_parquet_metrics(
        parquet_path,
        watermark_column="id",
        watermark_type=WatermarkType.INTEGER,
        incoming_watermark=incoming,
    )
    assert metrics["row_count"] == 0
    assert metrics["watermark"] == {
        "column": "id",
        "type": "integer",
        "value": "100",
    }


def test_omnibeam_poll_job_status_fails_when_manifest_had_watermark_but_metrics_omitted(
    tmp_path: Path,
) -> None:
    job_id = "job-wm-missing"
    job_dir = tmp_path / "p-1" / job_id
    job_dir.mkdir(parents=True)

    manifest = {
        "database_source": {"watermark": {"column": "id", "type": "integer", "value": "10"}}
    }
    (job_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    # Empty metrics without watermark
    metrics = {"row_count": 10, "rows_written": 10}
    (job_dir / "metrics.json").write_text(json.dumps(metrics), encoding="utf-8")

    # Create dummy parquet file so it treats job as finished
    table = pa.Table.from_pydict({"id": [11, 12]})
    pq.write_table(table, job_dir / "output.parquet")

    adapter = OmniBeamComputeAdapter(
        output_base_dir=tmp_path,
    )
    result = adapter.poll_job_status(job_id)
    assert result.status == JobStatus.FAILED
    assert "OmniBeam contract violation" in (result.error_message or "")


def test_omnibeam_poll_job_status_preserves_existing_watermark_in_metrics(tmp_path: Path) -> None:
    job_id = "job-wm-ok"
    job_dir = tmp_path / "p-1" / job_id
    job_dir.mkdir(parents=True)

    manifest = {
        "database_source": {"watermark": {"column": "id", "type": "integer", "value": "10"}}
    }
    (job_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    # Metrics with watermark
    metrics = {
        "row_count": 5,
        "rows_written": 5,
        "watermark": {"column": "id", "type": "integer", "value": "20"},
    }
    (job_dir / "metrics.json").write_text(json.dumps(metrics), encoding="utf-8")

    table = pa.Table.from_pydict({"id": [15, 20]})
    pq.write_table(table, job_dir / "output.parquet")

    adapter = OmniBeamComputeAdapter(
        output_base_dir=tmp_path,
    )
    result = adapter.poll_job_status(job_id)
    assert result.status == JobStatus.SUCCESS
    final_metrics = json.loads(Path(result.metrics_path).read_text("utf-8"))
    assert final_metrics["watermark"] == {
        "column": "id",
        "type": "integer",
        "value": "20",
    }
