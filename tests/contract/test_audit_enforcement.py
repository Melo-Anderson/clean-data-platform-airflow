import inspect

from fastapi.routing import APIRoute

from app.main import app


def test_all_mutating_endpoints_require_background_tasks() -> None:
    mutating_methods = {"POST", "PUT", "PATCH", "DELETE"}
    violating_endpoints = []

    for route in app.routes:
        if isinstance(route, APIRoute):
            if route.methods.intersection(mutating_methods):
                # Inspect signature
                sig = inspect.signature(route.endpoint)
                has_background_tasks = any(
                    param.annotation.__name__ == "BackgroundTasks"
                    if hasattr(param.annotation, "__name__")
                    else False
                    for param in sig.parameters.values()
                )
                if not has_background_tasks:
                    violating_endpoints.append(route.path)

    assert not violating_endpoints, (
        f"The following mutating endpoints do not declare 'BackgroundTasks' "
        f"for audit logging: {violating_endpoints}. "
        f"Contract requires all state-mutating APIs to perform audit logging."
    )
