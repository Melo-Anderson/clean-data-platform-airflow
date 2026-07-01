from __future__ import annotations

import pytest

from app.domain.shared.policy_tag import PolicyTag
from app.domain.shared.value_objects import (
    CredentialReference,
    CronSchedule,
    DiscoveryScope,
    EmailAddress,
)


class TestEmailAddress:
    def test_valid_email_creates_instance(self) -> None:
        addr = EmailAddress("user@company.com")
        assert addr.value == "user@company.com"

    def test_missing_at_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Invalid email"):
            EmailAddress("notanemail")

    def test_equality_is_by_value(self) -> None:
        assert EmailAddress("a@b.com") == EmailAddress("a@b.com")

    def test_inequality_with_different_value(self) -> None:
        assert EmailAddress("a@b.com") != EmailAddress("c@d.com")


class TestCredentialReference:
    def test_valid_path_creates_instance(self) -> None:
        ref = CredentialReference("vault/secret/oracle-prod")
        assert ref.path == "vault/secret/oracle-prod"

    def test_empty_path_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="cannot be empty"):
            CredentialReference("")

    def test_equality_by_path(self) -> None:
        assert CredentialReference("a/b") == CredentialReference("a/b")


class TestCronSchedule:
    def test_valid_5field_cron_creates_instance(self) -> None:
        sched = CronSchedule("0 6 * * *")
        assert sched.expression == "0 6 * * *"

    def test_invalid_text_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Invalid cron expression"):
            CronSchedule("not-a-cron")

    def test_6field_cron_is_rejected(self) -> None:
        # Platform uses standard 5-field cron only
        with pytest.raises(ValueError, match="Invalid cron expression"):
            CronSchedule("0 0 6 * * *")


class TestDiscoveryScope:
    def test_default_scope_is_all_inclusive(self) -> None:
        scope = DiscoveryScope()
        assert scope.include == ()
        assert scope.exclude == ()

    def test_scope_with_include_and_exclude(self) -> None:
        scope = DiscoveryScope(include=["customers", "orders"], exclude=["temp_*"])
        assert "customers" in scope.include
        assert "temp_*" in scope.exclude

    def test_is_immutable(self) -> None:
        scope = DiscoveryScope(include=["a"])
        with pytest.raises(AttributeError):
            scope.include = ["b"]  # type: ignore[misc]


class TestPolicyTag:
    def test_all_expected_tags_exist(self) -> None:
        assert set(PolicyTag) == {
            PolicyTag.PII,
            PolicyTag.RESTRICTED,
            PolicyTag.PUBLIC,
            PolicyTag.CONFIDENTIAL,
        }

    def test_pii_value_is_string(self) -> None:
        assert PolicyTag.PII.value == "PII"
