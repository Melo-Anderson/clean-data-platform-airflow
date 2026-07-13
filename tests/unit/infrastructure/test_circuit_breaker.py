from __future__ import annotations

import asyncio

import pytest

from app.domain.shared.exceptions import CircuitBreakerOpenError
from app.infrastructure.resilience.circuit_breaker import AsyncCircuitBreaker


async def _ok() -> str:
    return "ok"


async def _fail() -> None:
    raise RuntimeError("boom")


@pytest.mark.asyncio
async def test_closed_passes_through():
    cb = AsyncCircuitBreaker("test", failure_threshold=3)
    result = await cb.call(_ok())
    assert result == "ok"


@pytest.mark.asyncio
async def test_opens_after_threshold():
    cb = AsyncCircuitBreaker("test", failure_threshold=2)
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await cb.call(_fail())
    with pytest.raises(CircuitBreakerOpenError):
        await cb.call(_ok())


@pytest.mark.asyncio
async def test_half_open_probe_succeeds_closes_circuit():
    cb = AsyncCircuitBreaker("test", failure_threshold=1, recovery_timeout_seconds=0.01)
    with pytest.raises(RuntimeError):
        await cb.call(_fail())
    await asyncio.sleep(0.05)
    result = await cb.call(_ok())
    assert result == "ok"
    assert await cb.call(_ok()) == "ok"


@pytest.mark.asyncio
async def test_half_open_probe_fails_reopens_circuit():
    cb = AsyncCircuitBreaker("test", failure_threshold=1, recovery_timeout_seconds=0.01)
    with pytest.raises(RuntimeError):
        await cb.call(_fail())
    await asyncio.sleep(0.05)
    with pytest.raises(RuntimeError):
        await cb.call(_fail())
    with pytest.raises(CircuitBreakerOpenError):
        await cb.call(_ok())
