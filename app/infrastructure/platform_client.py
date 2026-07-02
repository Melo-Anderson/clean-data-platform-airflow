from __future__ import annotations

from datetime import datetime


class PlatformClient:
    """Stub for Task 10"""

    def pipeline_succeeded_on(
        self,
        pipeline_id: str,
        require_same_day: bool,
        logical_date: datetime,
        dependency_type: str,
    ) -> bool:
        return True


def get_platform_client() -> PlatformClient:
    return PlatformClient()
