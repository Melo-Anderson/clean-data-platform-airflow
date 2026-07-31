from __future__ import annotations

import pytest

from app.application.shared.adapters.dwh_provisioner_adapter import DwhProvisionerAdapter
from app.infrastructure.dwh_provisioners.noop_provisioner import NoOpDwhProvisioner


def test_noop_provisioner_implements_protocol():
    provisioner = NoOpDwhProvisioner()
    assert isinstance(provisioner, DwhProvisionerAdapter)


@pytest.mark.asyncio
async def test_noop_provisioner_methods_executes_without_error():
    provisioner = NoOpDwhProvisioner()
    await provisioner.ensure_dataset_exists(
        "demo_dataset", description="desc", labels={"env": "demo"}
    )
    await provisioner.ensure_table_exists(
        "demo_dataset", "demo_table", description="desc", labels={}, schema_fields=[]
    )
