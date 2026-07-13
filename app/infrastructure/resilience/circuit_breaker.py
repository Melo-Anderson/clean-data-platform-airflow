from __future__ import annotations

import enum
import logging
import time
from collections.abc import Coroutine
from typing import Any

from app.domain.shared.exceptions import CircuitBreakerOpenError

logger = logging.getLogger(__name__)


class _State(enum.Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class AsyncCircuitBreaker:
    """Lightweight async circuit breaker protecting a named external dependency.

    States:
        CLOSED: calls pass through; failures increment the counter.
        OPEN: calls fail fast with CircuitBreakerOpenError.
        HALF_OPEN: one probe call allowed; success -> CLOSED, failure -> OPEN.

    Usage:
        cb = AsyncCircuitBreaker("airflow-api", failure_threshold=5)
        result = await cb.call(some_async_fn())
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout_seconds: float = 30.0,
    ) -> None:
        self.name = name
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout_seconds
        self._state = _State.CLOSED
        self._failure_count = 0
        self._opened_at: float | None = None

    @property
    def state(self) -> str:
        return self._state.value

    async def call(self, coro: Coroutine[Any, Any, Any]) -> Any:
        """Execute the coroutine through the circuit breaker.

        Raises:
            CircuitBreakerOpenError: When the circuit is OPEN.
            Exception: Any exception raised by the coroutine.
        """
        if self._state == _State.OPEN:
            elapsed = time.monotonic() - (self._opened_at or 0)
            if elapsed >= self._recovery_timeout:
                self._transition_to(_State.HALF_OPEN)
            else:
                raise CircuitBreakerOpenError(self.name)

        try:
            result = await coro
            self._on_success()
            return result
        except Exception:
            self._on_failure()
            raise

    def _on_success(self) -> None:
        if self._state == _State.HALF_OPEN:
            logger.info("circuit_breaker.closed | name=%s", self.name)
        self._state = _State.CLOSED
        self._failure_count = 0
        self._opened_at = None

    def _on_failure(self) -> None:
        self._failure_count += 1
        if self._state == _State.HALF_OPEN or self._failure_count >= self._failure_threshold:
            self._transition_to(_State.OPEN)

    def _transition_to(self, state: _State) -> None:
        logger.warning("circuit_breaker.%s | name=%s", state.value.lower(), self.name)
        self._state = state
        if state == _State.OPEN:
            self._opened_at = time.monotonic()
            self._failure_count = 0
