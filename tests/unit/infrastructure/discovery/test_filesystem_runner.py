from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from app.domain.endpoints.endpoint import FileSystemEndpoint
from app.domain.shared.value_objects import CredentialReference
from app.infrastructure.discovery.filesystem_runner import FileSystemDiscoveryRunner


@pytest.fixture
def temp_data_dir(tmp_path: Path) -> Path:
    # 1. Simple CSV with comma
    csv_file = tmp_path / "customers.csv"
    csv_file.write_text(
        "id,name,email,created_at\n1,Alice,alice@co.com,2026-01-01\n2,Bob,bob@co.com,2026-01-02\n",
        encoding="utf-8",
    )

    # 2. Semicolon-delimited CSV with date suffix (for transactions mapping)
    semi_csv = tmp_path / "transac_20260801.csv"
    semi_csv.write_text(
        "tx_id;amount;status\n100;49.90;COMPLETED\n101;12.50;PENDING\n",
        encoding="utf-8",
    )

    # 3. Standard JSON array
    json_file = tmp_path / "products.json"
    json_file.write_text(
        json.dumps(
            [
                {"sku": "P1", "price": 99.0, "active": True},
                {"sku": "P2", "price": 149.0, "active": False},
            ]
        ),
        encoding="utf-8",
    )

    # 4. JSONLines (NDJSON) older file
    ndjson_file = tmp_path / "pedidos_20260801.json"
    ndjson_file.write_text(
        '{"order_id": "O1", "total": 100.0}\n{"order_id": "O2", "total": 250.0}\n',
        encoding="utf-8",
    )

    # 5. Newer pedidos file for testing latest mtime selection
    time.sleep(0.05)
    newer_pedidos = tmp_path / "pedidos_20260827.json"
    newer_pedidos.write_text(
        '{"order_id": "O3", "total": 300.0, "discount": 10.0}\n',
        encoding="utf-8",
    )

    # 6. Excluded file
    ignored_file = tmp_path / "temp_backup.csv"
    ignored_file.write_text("a,b\n1,2\n", encoding="utf-8")

    return tmp_path


@pytest.mark.asyncio
async def test_filesystem_runner_discovers_csv_and_json(temp_data_dir: Path) -> None:
    endpoint = FileSystemEndpoint(
        id="ep-fs",
        name="local-fs",
        credential_ref=CredentialReference("vault/none"),
        root_path=str(temp_data_dir),
    )
    runner = FileSystemDiscoveryRunner()
    snapshots = await runner.run(
        asset_id="asset-1",
        scope_include=["*customers*.csv", "*products*.json"],
        scope_exclude=[],
        endpoint=endpoint,
    )

    assert len(snapshots) == 2
    names = {s.object_name for s in snapshots}
    assert "customers" in names
    assert "products" in names

    cust_snap = next(s for s in snapshots if s.object_name == "customers")
    field_names = [f.name for f in cust_snap.fields]
    assert "id" in field_names
    assert "name" in field_names
    assert "email" in field_names


@pytest.mark.asyncio
async def test_filesystem_runner_explicit_depara_and_latest_mtime(temp_data_dir: Path) -> None:
    endpoint = FileSystemEndpoint(
        id="ep-fs",
        name="local-fs",
        credential_ref=CredentialReference("vault/none"),
        root_path=str(temp_data_dir),
    )
    runner = FileSystemDiscoveryRunner()
    snapshots = await runner.run(
        asset_id="asset-1",
        scope_include=["*pedidos*.json:pedidos", "*transac*.csv:transactions"],
        scope_exclude=[],
        endpoint=endpoint,
    )

    assert len(snapshots) == 2
    names = {s.object_name for s in snapshots}
    assert "pedidos" in names
    assert "transactions" in names

    pedidos_snap = next(s for s in snapshots if s.object_name == "pedidos")
    field_names = [f.name for f in pedidos_snap.fields]
    assert "discount" in field_names  # only in latest pedidos_20260827.json


@pytest.mark.asyncio
async def test_filesystem_runner_respects_scope_exclude(temp_data_dir: Path) -> None:
    endpoint = FileSystemEndpoint(
        id="ep-fs",
        name="local-fs",
        credential_ref=CredentialReference("vault/none"),
        root_path=str(temp_data_dir),
    )
    runner = FileSystemDiscoveryRunner()
    snapshots = await runner.run(
        asset_id="asset-1",
        scope_include=["*.csv"],
        scope_exclude=["*backup*", "*transac*"],
        endpoint=endpoint,
    )

    assert len(snapshots) == 1
    assert snapshots[0].object_name == "customers"
