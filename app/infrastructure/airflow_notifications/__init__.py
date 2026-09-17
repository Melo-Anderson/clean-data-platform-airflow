"""Airflow notification providers for Clean Data Platform."""

from __future__ import annotations

from app.infrastructure.airflow_notifications.platform_notification import (
    PlatformFailureNotification,
)

__all__ = ["PlatformFailureNotification"]
