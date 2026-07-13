# Engineering Rigor (Hypothesis, Chaos Testing & Mutation Testing) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add property-based testing (Hypothesis), chaos integration tests (respx fault injection), and mutation testing tooling (mutmut) to the platform with developer-local and CI integration.

**Architecture:** Three independent test suites share the existing pytest infrastructure. Property tests live in `tests/unit/domain/`, chaos tests in `tests/integration/chaos/`, and mutmut is configured via `setup.cfg` for manual local runs. All new tests run under `pytest -m "not e2e"` to remain CI-compatible.

**Tech Stack:** Python 3.12, pytest-asyncio, hypothesis, respx, mutmut, tenacity, uv

## Global Constraints

- Python >= 3.12; all imports use `from __future__ import annotations`
- All production code must pass `uv run mypy app/` with zero errors
- All tests must pass `uv run pytest tests/unit tests/integration -m "not e2e"`
- No new dependencies added to `[project.dependencies]` — dev-only deps go in `[project.optional-dependencies] dev`
- Follow existing naming: `test_<unit>_<behavior>` for all test functions
- Commit after every task: `feat:`, `test:`, `chore:` prefixes as appropriate

---

### Task 1: Add `hypothesis`, `respx`, and `mutmut` as dev dependencies

**Files:**
- Modify: `pyproject.toml`
- Modify: `setup.cfg` (create if absent)

**Interfaces:**
- Produces: `hypothesis` importable in tests; `respx` importable; `mutmut` CLI available via `uv run mutmut`

- [ ] **Step 1: Add dev dependencies to pyproject.toml**

Open `pyproject.toml`. Add to `[project.optional-dependencies] dev`:

```toml
[project.optional-dependencies]
dev = [
    "pytest>=8.2",
    "pytest-asyncio>=0.23",
    "pytest-cov>=5.0",
    "aiosqlite>=0.20",
    "ruff>=0.4",
    "mypy>=1.10",
    "pre-commit>=3.7",
    "hypothesis>=6.100",
    "respx>=0.21",
    "mutmut>=2.4",
]
```

- [ ] **Step 2: Sync dependencies**

```bash
uv sync --all-extras
```

Expected: resolves and installs `hypothesis`, `respx`, `mutmut` successfully.

- [ ] **Step 3: Create setup.cfg for mutmut config**

Create `setup.cfg` in the project root with:

```ini
[mutmut]
paths_to_mutate=app/domain/,app/application/
backup=False
runner=uv run pytest -m "not e2e" -x -q
tests_dir=tests/unit/
```

- [ ] **Step 4: Add Makefile target for mutation testing**

Open `Makefile`. Append:

```makefile
.PHONY: mutation-test
mutation-test:
	uv run mutmut run
	uv run mutmut results
```

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml setup.cfg Makefile uv.lock
git commit -m "chore: add hypothesis, respx, mutmut as dev dependencies"
```

---

### Task 2: Configure Hypothesis profiles in conftest.py

**Files:**
- Modify: `tests/conftest.py`

**Interfaces:**
- Consumes: nothing
- Produces: `settings` profiles `"dev"` and `"ci"` available for use in all Hypothesis tests via `@settings(profile="dev")`

- [ ] **Step 1: Add Hypothesis profile settings**

Open `tests/conftest.py`. Add after the existing imports at the top:

```python
from hypothesis import HealthCheck, settings

settings.register_profile("dev", max_examples=50, suppress_health_check=[HealthCheck.too_slow])
settings.register_profile("ci", max_examples=500, suppress_health_check=[HealthCheck.too_slow])
settings.load_profile(os.getenv("HYPOTHESIS_PROFILE", "dev"))
```

Also ensure `import os` is present in the imports section. The profile is automatically selected based on environment variable `HYPOTHESIS_PROFILE`. In CI, set `HYPOTHESIS_PROFILE=ci`.

- [ ] **Step 2: Verify profile loads without error**

```bash
uv run pytest tests/conftest.py --collect-only
```

Expected: no import errors; collection completes cleanly.

- [ ] **Step 3: Commit**

```bash
git add tests/conftest.py
git commit -m "chore: configure hypothesis dev/ci profiles in conftest"
```

---

### Task 3: Property-based tests for DiscoveryScope

**Files:**
- Create: `tests/unit/domain/test_discovery_scope_properties.py`

**Interfaces:**
- Consumes:
  - `DiscoveryScope` from `app.domain.shared.value_objects`
    - `DiscoveryScope(include: list[str] | None, exclude: list[str] | None)`
    - `DiscoveryScope.to_dict() -> dict[str, list[str]]`
    - `DiscoveryScope.from_dict(data: dict[str, list[str]]) -> DiscoveryScope`
- Produces: test file verifying roundtrip serialization and immutability invariants

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/domain/test_discovery_scope_properties.py`:

```python
from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from app.domain.shared.value_objects import DiscoveryScope

pattern_strategy = st.lists(st.text(min_size=1, max_size=30), max_size=10)


@given(include=pattern_strategy, exclude=pattern_strategy)
def test_discovery_scope_roundtrip_serialization(include: list[str], exclude: list[str]) -> None:
    """Serializing to dict and deserializing must yield an equal DiscoveryScope."""
    scope = DiscoveryScope(include=include, exclude=exclude)
    restored = DiscoveryScope.from_dict(scope.to_dict())
    assert restored == scope


@given(include=pattern_strategy, exclude=pattern_strategy)
def test_discovery_scope_input_mutation_does_not_affect_scope(
    include: list[str], exclude: list[str]
) -> None:
    """Mutating the input lists after construction must NOT alter the scope."""
    scope = DiscoveryScope(include=include, exclude=exclude)
    original_include = tuple(include)
    original_exclude = tuple(exclude)
    include.append("__mutated__")
    exclude.append("__mutated__")
    assert scope.include == original_include
    assert scope.exclude == original_exclude


@given(include=pattern_strategy)
def test_discovery_scope_empty_exclude_default(include: list[str]) -> None:
    """When exclude is None, the scope's exclude tuple must be empty."""
    scope = DiscoveryScope(include=include, exclude=None)
    assert scope.exclude == ()
```

- [ ] **Step 2: Run tests to verify they pass (roundtrip) and catch immutability invariant**

```bash
uv run pytest tests/unit/domain/test_discovery_scope_properties.py -v
```

Expected: All 3 tests PASS (DiscoveryScope already converts to tuples; roundtrip is implemented).

- [ ] **Step 3: Commit**

```bash
git add tests/unit/domain/test_discovery_scope_properties.py
git commit -m "test: add hypothesis property tests for DiscoveryScope"
```

---

### Task 4: Property-based tests for SchemaDiffer

**Files:**
- Create: `tests/unit/domain/test_schema_differ_properties.py`

**Interfaces:**
- Consumes:
  - `SchemaDiffer` from `app.domain.discovery.services.schema_differ`
    - `SchemaDiffer.diff(previous: SchemaSnapshot | None, current: SchemaSnapshot | None) -> list[DriftEvent]`
  - `SchemaSnapshot` from `app.domain.discovery.schema_snapshot`
    - `SchemaSnapshot(object_id: str, fields: list[SchemaField])`
  - `SchemaField` from `app.domain.discovery.schema_field`
    - `SchemaField(name: str, source_type: str, normalized_type: str, nullable: bool)`
  - `DriftChangeType` from `app.domain.discovery.drift_change_type`
    - `DriftChangeType.TYPE_INCOMPATIBLE`, `DriftChangeType.TYPE_WIDENED`
- Produces: tests verifying field-order commutativity and widening/incompatibility rules

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/domain/test_schema_differ_properties.py`:

```python
from __future__ import annotations

import random

from hypothesis import given, settings
from hypothesis import strategies as st

from app.domain.discovery.drift_change_type import DriftChangeType
from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.discovery.services.schema_differ import SchemaDiffer

differ = SchemaDiffer()

COMPATIBLE_PAIRS = [("integer", "bigint"), ("integer", "float"), ("bigint", "float")]
INCOMPATIBLE_PAIRS = [("integer", "string"), ("float", "string"), ("bigint", "boolean")]


def _make_field(name: str, ntype: str, nullable: bool = True) -> SchemaField:
    return SchemaField(name=name, source_type=ntype, normalized_type=ntype, nullable=nullable)


def _make_snapshot(object_id: str, fields: list[SchemaField]) -> SchemaSnapshot:
    return SchemaSnapshot(object_id=object_id, fields=fields)


field_name_strategy = st.text(alphabet=st.characters(whitelist_categories=("Ll",)), min_size=1, max_size=10)
ntype_strategy = st.sampled_from(["integer", "bigint", "float", "string", "decimal"])


@given(
    fields=st.lists(
        st.tuples(field_name_strategy, ntype_strategy),
        min_size=1,
        max_size=5,
        unique_by=lambda t: t[0],
    )
)
def test_schema_differ_field_order_commutativity(fields: list[tuple[str, str]]) -> None:
    """diff result must not depend on column declaration order."""
    schema_fields = [_make_field(name, ntype) for name, ntype in fields]
    shuffled = schema_fields[:]
    random.shuffle(shuffled)

    snap_a = _make_snapshot("obj_1", schema_fields)
    snap_b = _make_snapshot("obj_1", shuffled)

    # Diffing a snapshot against itself (same fields, different order) must produce no drift events.
    events = differ.diff(snap_a, snap_b)
    assert events == [], f"Expected no drift events for same-schema different-order, got: {events}"


@given(pair=st.sampled_from(COMPATIBLE_PAIRS))
def test_compatible_type_change_is_never_incompatible(pair: tuple[str, str]) -> None:
    """Widening type changes must produce TYPE_WIDENED, never TYPE_INCOMPATIBLE."""
    from_type, to_type = pair
    prev = _make_snapshot("obj", [_make_field("col", from_type)])
    curr = _make_snapshot("obj", [_make_field("col", to_type)])
    events = differ.diff(prev, curr)
    change_types = {e.change_type for e in events}
    assert DriftChangeType.TYPE_INCOMPATIBLE not in change_types
    assert DriftChangeType.TYPE_WIDENED in change_types


@given(pair=st.sampled_from(INCOMPATIBLE_PAIRS))
def test_incompatible_type_change_always_signals_incompatible(pair: tuple[str, str]) -> None:
    """Incompatible type changes must always produce TYPE_INCOMPATIBLE."""
    from_type, to_type = pair
    prev = _make_snapshot("obj", [_make_field("col", from_type)])
    curr = _make_snapshot("obj", [_make_field("col", to_type)])
    events = differ.diff(prev, curr)
    change_types = {e.change_type for e in events}
    assert DriftChangeType.TYPE_INCOMPATIBLE in change_types
```

- [ ] **Step 2: Run to verify tests pass**

```bash
uv run pytest tests/unit/domain/test_schema_differ_properties.py -v
```

Expected: All 3 tests PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/unit/domain/test_schema_differ_properties.py
git commit -m "test: add hypothesis property tests for SchemaDiffer"
```

---

### Task 5: Chaos tests for AirflowOrchestratorAdapter

**Files:**
- Create: `tests/integration/chaos/__init__.py`
- Create: `tests/integration/chaos/test_airflow_adapter_chaos.py`

**Interfaces:**
- Consumes:
  - `AirflowOrchestratorAdapter` from `app.infrastructure.adapters.orchestration.airflow_orchestrator_adapter`
    - `AirflowOrchestratorAdapter(airflow_url: str, username: str, password: str, circuit_breaker: AsyncCircuitBreaker | None)`
    - `AirflowOrchestratorAdapter.trigger_dag(pipeline_id: str, run_id: str, dag_run_id: str, pipeline_name: str) -> None`
  - `AsyncCircuitBreaker` from `app.infrastructure.resilience.circuit_breaker`
    - `AsyncCircuitBreaker(name: str, failure_threshold: int, recovery_timeout_seconds: float)`
  - `CircuitBreakerOpenError` from `app.domain.shared.exceptions`
- Produces: tests validating transient recovery and circuit breaker opening

- [ ] **Step 1: Create chaos test package**

```bash
# Create empty __init__.py for pytest discovery
```

Create `tests/integration/chaos/__init__.py` as an empty file.

- [ ] **Step 2: Write chaos tests**

Create `tests/integration/chaos/test_airflow_adapter_chaos.py`:

```python
from __future__ import annotations

import httpx
import pytest
import respx

from app.domain.shared.exceptions import CircuitBreakerOpenError
from app.infrastructure.adapters.orchestration.airflow_orchestrator_adapter import (
    AirflowOrchestratorAdapter,
)
from app.infrastructure.resilience.circuit_breaker import AsyncCircuitBreaker

AIRFLOW_URL = "http://fake-airflow:8080"
TOKEN_URL = f"{AIRFLOW_URL}/auth/token"
DAG_RUN_URL = f"{AIRFLOW_URL}/api/v2/dags/my_dag/dagRuns"


@pytest.fixture
def adapter_without_cb() -> AirflowOrchestratorAdapter:
    return AirflowOrchestratorAdapter(
        airflow_url=AIRFLOW_URL,
        username="admin",
        password="admin",
        max_retries=1,
        retry_delay_seconds=0.0,
    )


@pytest.fixture
def circuit_breaker() -> AsyncCircuitBreaker:
    return AsyncCircuitBreaker("airflow-test", failure_threshold=3, recovery_timeout_seconds=999)


@pytest.fixture
def adapter_with_cb(circuit_breaker: AsyncCircuitBreaker) -> AirflowOrchestratorAdapter:
    return AirflowOrchestratorAdapter(
        airflow_url=AIRFLOW_URL,
        username="admin",
        password="admin",
        max_retries=1,
        retry_delay_seconds=0.0,
        circuit_breaker=circuit_breaker,
    )


@pytest.mark.asyncio
@respx.mock
async def test_trigger_dag_succeeds_on_first_attempt(
    adapter_without_cb: AirflowOrchestratorAdapter,
) -> None:
    """When Airflow returns 201, trigger_dag completes without error."""
    respx.post(TOKEN_URL).mock(return_value=httpx.Response(200, json={"access_token": "tok"}))
    respx.post(DAG_RUN_URL).mock(return_value=httpx.Response(201))

    await adapter_without_cb.trigger_dag(
        pipeline_id="pid", run_id="rid", dag_run_id="drid", pipeline_name="my_dag"
    )


@pytest.mark.asyncio
@respx.mock
async def test_circuit_breaker_opens_after_threshold_failures(
    adapter_with_cb: AirflowOrchestratorAdapter,
    circuit_breaker: AsyncCircuitBreaker,
) -> None:
    """After failure_threshold consecutive errors, the circuit opens and subsequent calls raise CircuitBreakerOpenError instantly."""
    respx.post(TOKEN_URL).mock(return_value=httpx.Response(200, json={"access_token": "tok"}))
    respx.post(DAG_RUN_URL).mock(return_value=httpx.Response(503))

    # Exhaust the threshold (3 failures)
    for _ in range(3):
        with pytest.raises(Exception):
            await adapter_with_cb.trigger_dag(
                pipeline_id="pid", run_id="rid", dag_run_id="drid", pipeline_name="my_dag"
            )

    assert circuit_breaker.state == "OPEN"

    # The next call must fail fast — no HTTP call should be made
    with pytest.raises(CircuitBreakerOpenError):
        await adapter_with_cb.trigger_dag(
            pipeline_id="pid", run_id="rid", dag_run_id="drid", pipeline_name="my_dag"
        )


@pytest.mark.asyncio
@respx.mock
async def test_trigger_dag_raises_on_persistent_error(
    adapter_without_cb: AirflowOrchestratorAdapter,
) -> None:
    """Persistent 503 responses raise an httpx.HTTPStatusError (no silent failure)."""
    respx.post(TOKEN_URL).mock(return_value=httpx.Response(200, json={"access_token": "tok"}))
    respx.post(DAG_RUN_URL).mock(return_value=httpx.Response(503))

    with pytest.raises(httpx.HTTPStatusError):
        await adapter_without_cb.trigger_dag(
            pipeline_id="pid", run_id="rid", dag_run_id="drid", pipeline_name="my_dag"
        )
```

- [ ] **Step 3: Run tests**

```bash
uv run pytest tests/integration/chaos/test_airflow_adapter_chaos.py -v
```

Expected: All 3 tests PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/integration/chaos/__init__.py tests/integration/chaos/test_airflow_adapter_chaos.py
git commit -m "test: add chaos integration tests for AirflowOrchestratorAdapter"
```

---

### Task 6: Chaos tests for BaoSecretManagerAdapter

**Files:**
- Create: `tests/integration/chaos/test_vault_adapter_chaos.py`

**Interfaces:**
- Consumes:
  - `BaoSecretManagerAdapter` from `app.infrastructure.adapters.secrets.bao_secret_manager_adapter`
    - `BaoSecretManagerAdapter(vault_url: str, vault_token: str)`
    - `BaoSecretManagerAdapter.resolve(ref: str) -> dict[str, str]`
- Produces: tests validating retry-on-transient-error and hard-failure on 500

- [ ] **Step 1: Write chaos tests for Vault adapter**

Create `tests/integration/chaos/test_vault_adapter_chaos.py`:

```python
from __future__ import annotations

import httpx
import pytest
import respx

from app.infrastructure.adapters.secrets.bao_secret_manager_adapter import BaoSecretManagerAdapter

VAULT_URL = "http://fake-vault:8200"
SECRET_PATH = "secret/data/my-db"
VAULT_API_URL = f"{VAULT_URL}/v1/{SECRET_PATH}"


@pytest.fixture
def adapter() -> BaoSecretManagerAdapter:
    return BaoSecretManagerAdapter(vault_url=VAULT_URL, vault_token="root")


@pytest.mark.asyncio
@respx.mock
async def test_vault_resolve_succeeds(adapter: BaoSecretManagerAdapter) -> None:
    """When Vault returns 200 with KV v2 payload, resolve returns the secret dict."""
    respx.get(VAULT_API_URL).mock(
        return_value=httpx.Response(
            200,
            json={"data": {"data": {"user": "admin", "password": "secret123"}}},
        )
    )

    result = await adapter.resolve(SECRET_PATH)
    assert result == {"user": "admin", "password": "secret123"}


@pytest.mark.asyncio
@respx.mock
async def test_vault_returns_runtime_error_on_500(adapter: BaoSecretManagerAdapter) -> None:
    """Persistent 500 from Vault exhausts retries and raises RuntimeError."""
    respx.get(VAULT_API_URL).mock(return_value=httpx.Response(500, text="Internal Server Error"))

    with pytest.raises(RuntimeError, match="OpenBao request failed with status 500"):
        await adapter.resolve(SECRET_PATH)


@pytest.mark.asyncio
@respx.mock
async def test_vault_returns_key_error_on_404(adapter: BaoSecretManagerAdapter) -> None:
    """A 404 from Vault raises KeyError with the secret ref in the message."""
    respx.get(VAULT_API_URL).mock(return_value=httpx.Response(404))

    with pytest.raises(KeyError, match=SECRET_PATH):
        await adapter.resolve(SECRET_PATH)
```

- [ ] **Step 2: Run tests**

```bash
uv run pytest tests/integration/chaos/test_vault_adapter_chaos.py -v
```

Expected: All 3 tests PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/chaos/test_vault_adapter_chaos.py
git commit -m "test: add chaos integration tests for BaoSecretManagerAdapter (Vault)"
```

---

### Task 7: Update CI pipeline to use HYPOTHESIS_PROFILE=ci and run chaos tests

**Files:**
- Modify: `.github/workflows/ci_cd_pipeline.yml`

**Interfaces:**
- Consumes: existing `ci_gate` job in `.github/workflows/ci_cd_pipeline.yml`
- Produces: CI now runs Hypothesis in `ci` profile and includes chaos tests in the test run

- [ ] **Step 1: Update the unit test step in ci_gate**

Open `.github/workflows/ci_cd_pipeline.yml`. Find the step:

```yaml
    - name: Run Unit Tests with Coverage
      run: uv run pytest -m "not e2e" -v --cov=app --cov-report=xml --cov-report=term-missing --cov-fail-under=80
```

Replace it with:

```yaml
    - name: Run Unit Tests with Coverage
      env:
        HYPOTHESIS_PROFILE: ci
      run: uv run pytest -m "not e2e" -v --cov=app --cov-report=xml --cov-report=term-missing --cov-fail-under=80
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/ci_cd_pipeline.yml
git commit -m "chore: set HYPOTHESIS_PROFILE=ci in GitHub Actions CI gate"
```

---

### Task 8: Run full validation and push

**Files:**
- None (validation only)

- [ ] **Step 1: Run the full non-E2E test suite**

```bash
uv run pytest tests/unit tests/integration -m "not e2e" -v
```

Expected: All tests pass including new property and chaos tests.

- [ ] **Step 2: Run mypy**

```bash
uv run mypy app/
```

Expected: `Success: no issues found in N source files`

- [ ] **Step 3: Run ruff**

```bash
uv run ruff check . && uv run ruff format --check .
```

Expected: No errors, all files already formatted.

- [ ] **Step 4: Trial mutation test run on value_objects**

```bash
uv run mutmut run --paths-to-mutate app/domain/shared/value_objects.py
uv run mutmut results
```

Expected: mutmut generates mutants and shows surviving/killed counts. Review any survivors and decide if a new assertion is worth adding.

- [ ] **Step 5: Push branch**

```bash
git push
```
