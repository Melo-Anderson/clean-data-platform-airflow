from __future__ import annotations

import pytest

from app.domain.objects.data_object_metadata import (
    CompositeForeignKey,
    CompositeIndex,
    DataObjectMetadata,
)


def test_composite_index_defaults():
    idx = CompositeIndex(name="idx_pk", columns=["id"])
    assert idx.unique is False
    assert idx.columns == ["id"]


def test_composite_index_unique():
    idx = CompositeIndex(name="idx_email_tenant", columns=["email", "tenant_id"], unique=True)
    assert idx.unique is True
    assert len(idx.columns) == 2


def test_composite_foreign_key():
    fk = CompositeForeignKey(
        name="fk_users_tenant",
        constrained_columns=["tenant_id"],
        referred_table="tenants",
        referred_columns=["id"],
    )
    assert fk.referred_table == "tenants"
    assert fk.constrained_columns == ["tenant_id"]


def test_data_object_metadata_defaults():
    meta = DataObjectMetadata()
    assert meta.indexes == []
    assert meta.foreign_keys == []
    assert meta.partition_key is None


def test_data_object_metadata_full():
    idx = CompositeIndex(name="idx_user_email_tenant", columns=["email", "tenant_id"], unique=True)
    fk = CompositeForeignKey(
        name="fk_tenant",
        constrained_columns=["tenant_id"],
        referred_table="tenants",
        referred_columns=["id"],
    )
    meta = DataObjectMetadata(indexes=[idx], foreign_keys=[fk], partition_key="created_at")
    assert len(meta.indexes) == 1
    assert meta.indexes[0].name == "idx_user_email_tenant"
    assert meta.foreign_keys[0].referred_table == "tenants"
    assert meta.partition_key == "created_at"


def test_data_object_metadata_is_frozen():
    meta = DataObjectMetadata()
    with pytest.raises((AttributeError, TypeError)):
        meta.partition_key = "x"  # type: ignore[misc]
