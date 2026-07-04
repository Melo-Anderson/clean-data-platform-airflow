from __future__ import annotations

import logging

from app.infrastructure.adapters.notifications.notification_adapter import AlertLevel

logger = logging.getLogger(__name__)


class NoopNotificationAdapter:
    async def send_alert(self, channel: str, title: str, message: str, level: AlertLevel) -> None:
        logger.debug("NoopNotificationAdapter: [%s] %s — %s", level, title, message)

    def send_alert_sync(self, channel: str, title: str, message: str, level: AlertLevel) -> None:
        logger.debug("NoopNotificationAdapter(sync): [%s] %s — %s", level, title, message)
