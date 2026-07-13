# Design Specification: Engineering Rigor (Hypothesis, Chaos Testing & Mutation Testing)

**Date:** 2026-07-13
**Area:** Testing / Quality Assurance / CI-CD / Resiliency
**Status:** Under Review
**Group:** 3 (World-Class Engineering)

---

## 1. Context & Motivation

To establish a world-class level of engineering rigor on the Data Platform, we need to move beyond basic test coverage. We will implement three advanced testing practices:
1. **Property-Based Testing (Hypothesis):** Validate domain boundary invariants against hundreds of generated inputs.
2. **Chaos Integration Testing:** Test system resilience under simulated network timeouts, service errors (Airflow, Vault), and database locking.
3. **Mutation Testing (mutmut):** Audit unit test quality by intentionally injecting faults (mutants) and ensuring tests fail, focusing on pure domain logic.

These checks will be integrated into the local developer lifecycle (Git Hooks) and the CI pipeline to prevent regressions before they reach production.

---

## 2. Property-Based Testing (Hypothesis)

We will use the `hypothesis` framework to run properties validation against domain Value Objects and pure logic.

### 2.1 Target Components
- **DiscoveryScope (`app/domain/shared/value_objects.py`):**
  - Verify serialization roundtrip: `DiscoveryScope.from_dict(scope.to_dict()) == scope` for any generated lists of string patterns.
  - Verify immutability: Ensure mutating input lists after scope instantiation does not alter the `DiscoveryScope`'s internal `include` or `exclude` tuples.
- **SchemaDiffer (`app/domain/discovery/schema_differ.py`):**
  - Generate arbitrary schema definitions (varying data types, column names, nullability).
  - Verify commutative property: `diff(A, B)` results in the same drift classifications regardless of column declaration order.
  - Verify type compatibility rules: Widening is never classified as `incompatible`, while incompatible type changes always trigger the `incompatible` status.

### 2.2 Hypothesis Profiles
Configure settings in `tests/conftest.py` to adapt execution time to the context:
- **`dev` profile (local pre-push):** Runs 50 test cases per property to keep runs fast.
- **`ci` profile (GitHub Actions):** Runs 500 test cases per property for thorough edge-case discovery.

---

## 3. Chaos Integration Testing

We will simulate network instability and backend outages in integration tests under `tests/integration/chaos/`.

### 3.1 Simulated Faults
- **Airflow REST Client:** Use `respx` to mock the Airflow API. Introduce simulated latencies (e.g. 5 seconds) to trigger HTTP timeouts, and transient status codes (502 Bad Gateway, 503 Service Unavailable).
- **Vault Secret Manager:** Simulate communication drops (500 Internal Server Error) during credential resolution.
- **Database Locks:** Simulate database concurrency locks (`sqlite3.OperationalError: database is locked`) during write operations to verify transaction integrity and state machine recovery.

### 3.2 Resilience Invariants
Tests must assert:
- **Transient Recovery:** If an external dependency fails twice but succeeds on the third attempt, the decorator (`tenacity` retry) hides the failure, and the operation completes.
- **Circuit Breaker Activation:** If failures persist past the configured threshold (e.g., 5 failures), the circuit breaker opens. Subsequent calls fail instantly with a domain exception (`PlatformForbiddenError` or dedicated exception) without attempting I/O, preventing thread starvation.
- **Clean Errors (RFC 7807):** Verify that catastrophic failures do not leak connection strings or stack traces, yielding a valid JSON problem payload.

---

## 4. Mutation Testing (mutmut)

Mutation testing will be used locally by developers to identify untested code paths and weak assertions.

### 4.1 Configuration (`setup.cfg`)
We configure `mutmut` to target business logic only, avoiding slow runs on framework configuration or routes:
```ini
[mutmut]
paths_to_mutate=app/domain/,app/application/
backup=False
runner=uv run pytest -m "not e2e"
tests_dir=tests/unit/
```

### 4.2 Developer Loop
- Run locally: `uv run mutmut run` (or via Makefile target).
- Check survived mutants: `uv run mutmut show <mutant-id>` to see the exact code line mutation that went unnoticed by the tests.
- Resolution: The developer must strengthen test assertions or write target unit tests to kill the mutant before submitting a PR.

---

## 5. CI/CD & Git Hooks Integration

- **`pre-commit` hook:** Runs fast static analysis (Ruff formatting, Ruff linting) on staged files.
- **`pre-push` hook:** Runs `mypy` (via `uv run mypy`), Hypothesis tests, and Chaos tests before push.
- **CI Pipeline:** Executes static analysis, checks, and all non-E2E tests (`pytest -m "not e2e"`). The Hypothesis profile is automatically switched to `ci`.

---

## 6. Verification Plan

### Automated Verification
- Run unit/integration tests: `uv run pytest tests/unit tests/integration`
- Validate local hooks: Run `uv run pre-commit run --all-files --hook-type pre-push`
- Verify mypy type checks pass: `uv run mypy app/`
- Run a trial mutation check on a small domain module: `uv run mutmut run --paths-to-mutate app/domain/shared/value_objects.py`
