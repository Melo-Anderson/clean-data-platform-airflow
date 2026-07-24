from __future__ import annotations

from typing import Literal, Protocol, runtime_checkable

AlertLevel = Literal["info", "warning", "critical"]


@runtime_checkable
class NotificationAdapter(Protocol):
    async def send_alert(self, channel: str, title: str, message: str, level: AlertLevel) -> None:
        ...

    def send_alert_sync(self, channel: str, title: str, message: str, level: AlertLevel) -> None:
        ...
