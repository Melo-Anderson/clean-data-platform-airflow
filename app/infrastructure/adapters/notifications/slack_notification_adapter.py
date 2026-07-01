from __future__ import annotations

from app.infrastructure.adapters.notifications.notification_adapter import AlertLevel


class SlackNotificationAdapter:
    """Slack Incoming Webhooks integration. Configure via PLATFORM_SLACK_WEBHOOK_URL."""

    def __init__(self, webhook_url: str) -> None:
        self._webhook_url = webhook_url

    async def send_alert(self, channel: str, title: str, message: str, level: AlertLevel) -> None:
        raise NotImplementedError("SlackNotificationAdapter.send_alert not yet implemented")
