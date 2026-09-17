# tests/unit/domain/test_value_objects_property.py
from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from app.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress

# --- EmailAddress ---


@given(st.emails())
def test_email_address_accepts_valid_email(email: str) -> None:
    """EmailAddress deve aceitar qualquer email sintaticamente valido gerado pelo Hypothesis."""
    addr = EmailAddress(email)
    assert addr.value == email


@given(st.text().filter(lambda s: "@" not in s and s.strip() != ""))
def test_email_address_rejects_strings_without_at(s: str) -> None:
    """EmailAddress deve rejeitar qualquer string sem '@'."""
    with pytest.raises(ValueError):
        EmailAddress(s)


# --- CronSchedule ---

VALID_CRON = st.builds(
    lambda m, h, d, mo, w: f"{m} {h} {d} {mo} {w}",
    st.integers(min_value=0, max_value=59),
    st.integers(min_value=0, max_value=23),
    st.integers(min_value=1, max_value=28),
    st.integers(min_value=1, max_value=12),
    st.integers(min_value=0, max_value=6),
)


@given(VALID_CRON)
def test_cron_schedule_accepts_valid_5_field_expression(expr: str) -> None:
    sched = CronSchedule(expr)
    assert sched.expression == expr


@given(st.text().filter(lambda s: len(s.split()) != 5))
def test_cron_schedule_rejects_non_5_field_strings(s: str) -> None:
    with pytest.raises(ValueError):
        CronSchedule(s)


# --- DiscoveryScope ---


@given(st.lists(st.text(min_size=1)), st.lists(st.text(min_size=1)))
def test_discovery_scope_is_immutable(include: list[str], exclude: list[str]) -> None:
    """DiscoveryScope e um frozen dataclass — qualquer setattr levanta AttributeError."""
    scope = DiscoveryScope(include=include, exclude=exclude)
    with pytest.raises((AttributeError, TypeError)):
        scope.include = ("new",)  # type: ignore[misc]


@given(st.lists(st.text(min_size=1)), st.lists(st.text(min_size=1)))
def test_discovery_scope_roundtrips_through_dict(include: list[str], exclude: list[str]) -> None:
    """to_dict() e from_dict() devem ser inversos perfeitos."""
    scope = DiscoveryScope(include=include, exclude=exclude)
    restored = DiscoveryScope.from_dict(scope.to_dict())
    assert restored.include == scope.include
    assert restored.exclude == scope.exclude
