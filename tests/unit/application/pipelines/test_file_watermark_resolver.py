from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from app.application.pipelines.file_watermark_resolver import FileWatermarkResolver
from app.domain.endpoints.endpoint import FileSystemEndpoint
from app.domain.shared.value_objects import CredentialReference


class MockPipelineRunRepo:
    def __init__(self, latest_run=None, processed_hashes=None):
        self.latest_run = latest_run
        self.processed_hashes = processed_hashes or set()

    async def find_latest_by_pipeline_id(self, pipeline_id: str):
        return self.latest_run

    async def find_processed_hashes_by_pipeline(self, pipeline_id: str):
        return self.processed_hashes


class MockUoW:
    def __init__(self, latest_run=None, processed_hashes=None):
        self.pipeline_runs = MockPipelineRunRepo(latest_run, processed_hashes)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def commit(self):
        pass

    async def rollback(self):
        pass


@pytest.fixture
def sample_files_dir(tmp_path: Path) -> Path:
    f1 = tmp_path / "orders_001.csv"
    f1.write_text("id,total\n1,10.0\n", encoding="utf-8")
    f2 = tmp_path / "orders_002.csv"
    f2.write_text("id,total\n2,20.0\n", encoding="utf-8")
    return tmp_path


@pytest.mark.asyncio
async def test_resolve_pending_files_first_run(sample_files_dir: Path) -> None:
    uow = MockUoW()

    resolver = FileWatermarkResolver(uow=uow)
    endpoint = FileSystemEndpoint(
        id="ep-1",
        name="fs",
        credential_ref=CredentialReference("vault/none"),
        root_path=str(sample_files_dir),
    )

    files = await resolver.resolve_pending_files(
        pipeline_id="pipe-1",
        run_id="run-1",
        endpoint=endpoint,
        scope_include=["*.csv"],
        scope_exclude=[],
    )

    assert len(files) == 2
    names = {f.file_name for f in files}
    assert "orders_001.csv" in names
    assert "orders_002.csv" in names
    assert all(f.pipeline_run_id == "run-1" for f in files)


@pytest.mark.asyncio
async def test_resolve_pending_files_skips_already_processed_hash(sample_files_dir: Path) -> None:
    f1_content = (sample_files_dir / "orders_001.csv").read_bytes()
    f1_md5 = hashlib.md5(f1_content).hexdigest()

    uow = MockUoW(processed_hashes={f1_md5})

    resolver = FileWatermarkResolver(uow=uow)
    endpoint = FileSystemEndpoint(
        id="ep-1",
        name="fs",
        credential_ref=CredentialReference("vault/none"),
        root_path=str(sample_files_dir),
    )

    files = await resolver.resolve_pending_files(
        pipeline_id="pipe-1",
        run_id="run-2",
        endpoint=endpoint,
        scope_include=["*.csv"],
        scope_exclude=[],
    )

    assert len(files) == 1
    assert files[0].file_name == "orders_002.csv"
