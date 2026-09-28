# app/infrastructure/resilience/http_resilience.py
from __future__ import annotations

import functools
import logging
from collections.abc import Callable
from typing import Any, TypeVar, cast

T = TypeVar("T")


def resilient_http(fallback: Any = None, log_prefix: str = "HTTP call") -> Callable:
    """Silencia excecoes de rede e retorna fallback com log de warning.

    Uso:
        @resilient_http(fallback=False, log_prefix="pipeline_succeeded_on")
        def pipeline_succeeded_on(self, ...) -> bool:
            ...
    """

    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            try:
                return fn(*args, **kwargs)
            except Exception as exc:
                logging.getLogger(fn.__module__).warning("%s failed: %s", log_prefix, exc)
                return cast(T, fallback)

        return wrapper

    return decorator
