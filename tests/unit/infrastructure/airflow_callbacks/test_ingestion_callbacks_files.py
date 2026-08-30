from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import MagicMock, patch

from app.infrastructure.airflow_callbacks.ingestion_callbacks import resolve_source_files


def test_resolve_source_files_identifies_new_files_and_skips_processed_hashes(
    tmp_path: Path,
) -> None:
    f1 = tmp_path / "transactions_01.csv"
    f1.write_text("tx_id,amount\n1,100.0\n", encoding="utf-8")
    hash1 = hashlib.md5(f1.read_bytes()).hexdigest()

    f2 = tmp_path / "transactions_02.csv"
    f2.write_text("tx_id,amount\n2,200.0\n", encoding="utf-8")
    hash2 = hashlib.md5(f2.read_bytes()).hexdigest()

    # Mock platform client returning hash1 as already processed
    mock_client = MagicMock()
    mock_client.get_processed_hashes.return_value = {hash1}

    with patch(
        "app.infrastructure.airflow_callbacks.ingestion_callbacks.get_platform_client",
        return_value=mock_client,
    ):
        pending = resolve_source_files(
            pipeline_id="pipe-transactions",
            source_objects=[{"object_id": "asset-platform.transactions"}],
            landing_dir=str(tmp_path),
        )

    # hash1 must be skipped, only hash2 returned
    assert len(pending) == 1
    assert pending[0]["file_name"] == "transactions_02.csv"
    assert pending[0]["hash_md5"] == hash2
    assert pending[0]["status"] == "PROCESSED"
