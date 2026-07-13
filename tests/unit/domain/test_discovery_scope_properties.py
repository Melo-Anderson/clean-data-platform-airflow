from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from app.domain.shared.value_objects import DiscoveryScope

pattern_strategy = st.lists(st.text(min_size=1, max_size=30), max_size=10)


@given(include=pattern_strategy, exclude=pattern_strategy)
def test_discovery_scope_roundtrip_serialization(include: list[str], exclude: list[str]) -> None:
    """Serializing to dict and deserializing must yield an equal DiscoveryScope."""
    scope = DiscoveryScope(include=include, exclude=exclude)
    restored = DiscoveryScope.from_dict(scope.to_dict())
    assert restored == scope


@given(include=pattern_strategy, exclude=pattern_strategy)
def test_discovery_scope_input_mutation_does_not_affect_scope(
    include: list[str], exclude: list[str]
) -> None:
    """Mutating the input lists after construction must NOT alter the scope."""
    scope = DiscoveryScope(include=include, exclude=exclude)
    original_include = tuple(include)
    original_exclude = tuple(exclude)
    include.append("__mutated__")
    exclude.append("__mutated__")
    assert scope.include == original_include
    assert scope.exclude == original_exclude


@given(include=pattern_strategy)
def test_discovery_scope_empty_exclude_default(include: list[str]) -> None:
    """When exclude is None, the scope's exclude tuple must be empty."""
    scope = DiscoveryScope(include=include, exclude=None)
    assert scope.exclude == ()
