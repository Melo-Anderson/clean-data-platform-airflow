from __future__ import annotations

# Re-exports from the canonical application port for backwards compatibility.
from app.application.shared.ports.notification_port import AlertLevel, NotificationPort

__all__ = ["AlertLevel", "NotificationPort", "NotificationAdapter"]

# Alias so existing code using "NotificationAdapter" still works
NotificationAdapter = NotificationPort
