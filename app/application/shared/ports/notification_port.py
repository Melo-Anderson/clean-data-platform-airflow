from __future__ import annotations

from typing import Literal, Protocol, runtime_checkable

AlertLevel = Literal["info", "warning", "critical"]


@runtime_checkable
class NotificationPort(Protocol):
    """Port for sending asynchronous and synchronous notifications/alerts.

    Implementations live in app.infrastructure.adapters.notifications.
    """

    async def send_alert(
        self, channel: str, title: str, message: str, level: AlertLevel
    ) -> None: ...

    def send_alert_sync(
        self, channel: str, title: str, message: str, level: AlertLevel
    ) -> None: ...
