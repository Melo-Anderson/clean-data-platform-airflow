from __future__ import annotations

import random

from hypothesis import given
from hypothesis import strategies as st

from app.domain.discovery.drift_change_type import DriftChangeType
from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.discovery.services.schema_differ import SchemaDiffer

differ = SchemaDiffer()

COMPATIBLE_PAIRS = [("integer", "bigint"), ("integer", "float"), ("bigint", "float")]
INCOMPATIBLE_PAIRS = [("integer", "string"), ("float", "string"), ("bigint", "boolean")]


def _make_field(name: str, ntype: str, nullable: bool = True) -> SchemaField:
    return SchemaField(name=name, source_type=ntype, normalized_type=ntype, nullable=nullable)


def _make_snapshot(object_id: str, fields: list[SchemaField]) -> SchemaSnapshot:
    return SchemaSnapshot(object_id=object_id, fields=fields)


field_name_strategy = st.text(
    alphabet=st.characters(whitelist_categories=("Ll",)), min_size=1, max_size=10
)
ntype_strategy = st.sampled_from(["integer", "bigint", "float", "string", "decimal"])


@given(
    fields=st.lists(
        st.tuples(field_name_strategy, ntype_strategy),
        min_size=1,
        max_size=5,
        unique_by=lambda t: t[0],
    )
)
def test_schema_differ_field_order_commutativity(fields: list[tuple[str, str]]) -> None:
    """diff result must not depend on column declaration order."""
    schema_fields = [_make_field(name, ntype) for name, ntype in fields]
    shuffled = schema_fields[:]
    random.shuffle(shuffled)

    snap_a = _make_snapshot("obj_1", schema_fields)
    snap_b = _make_snapshot("obj_1", shuffled)

    # Diffing a snapshot against itself (same fields, different order) must produce no drift events.
    events = differ.diff(snap_a, snap_b)
    assert events == [], f"Expected no drift events for same-schema different-order, got: {events}"


@given(pair=st.sampled_from(COMPATIBLE_PAIRS))
def test_compatible_type_change_is_never_incompatible(pair: tuple[str, str]) -> None:
    """Widening type changes must produce TYPE_WIDENED, never TYPE_INCOMPATIBLE."""
    from_type, to_type = pair
    prev = _make_snapshot("obj", [_make_field("col", from_type)])
    curr = _make_snapshot("obj", [_make_field("col", to_type)])
    events = differ.diff(prev, curr)
    change_types = {e.change_type for e in events}
    assert DriftChangeType.TYPE_INCOMPATIBLE not in change_types
    assert DriftChangeType.TYPE_WIDENED in change_types


@given(pair=st.sampled_from(INCOMPATIBLE_PAIRS))
def test_incompatible_type_change_always_signals_incompatible(pair: tuple[str, str]) -> None:
    """Incompatible type changes must always produce TYPE_INCOMPATIBLE."""
    from_type, to_type = pair
    prev = _make_snapshot("obj", [_make_field("col", from_type)])
    curr = _make_snapshot("obj", [_make_field("col", to_type)])
    events = differ.diff(prev, curr)
    change_types = {e.change_type for e in events}
    assert DriftChangeType.TYPE_INCOMPATIBLE in change_types
