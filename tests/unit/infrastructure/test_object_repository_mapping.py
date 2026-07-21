"""Tests for DataObjectMetadata JSON serialization round-trip in the repository."""

from __future__ import annotations

from app.domain.objects.data_object_metadata import (
    CompositeForeignKey,
    CompositeIndex,
    DataObjectMetadata,
)
from app.infrastructure.persistence.repositories.sql_data_object_repository import (
    _dict_to_metadata,
    _metadata_to_dict,
)


def test_metadata_to_dict_none():
    assert _metadata_to_dict(None) is None


def test_metadata_to_dict_full():
    meta = DataObjectMetadata(
        indexes=[CompositeIndex(name="idx_ab", columns=["a", "b"], unique=True)],
        foreign_keys=[
            CompositeForeignKey(
                name="fk_x",
                constrained_columns=["a"],
                referred_table="tbl",
                referred_columns=["id"],
            )
        ],
        partition_key="created_at",
    )
    d = _metadata_to_dict(meta)
    assert d is not None
    assert d["partition_key"] == "created_at"
    assert d["indexes"][0]["name"] == "idx_ab"
    assert d["indexes"][0]["unique"] is True
    assert d["foreign_keys"][0]["referred_table"] == "tbl"


def test_dict_to_metadata_none():
    assert _dict_to_metadata(None) is None


def test_dict_to_metadata_round_trip():
    meta = DataObjectMetadata(
        indexes=[CompositeIndex(name="idx_ab", columns=["a", "b"], unique=True)],
        foreign_keys=[
            CompositeForeignKey(
                name="fk_x",
                constrained_columns=["a"],
                referred_table="tbl",
                referred_columns=["id"],
            )
        ],
        partition_key="ts",
    )
    restored = _dict_to_metadata(_metadata_to_dict(meta))
    assert restored == meta


def test_dict_to_metadata_empty_dict():
    restored = _dict_to_metadata({})
    assert restored == DataObjectMetadata()
