# Harness Engine Refactoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor `services/harness-engine` to use an agnostic LLM factory (`init_chat_model`), inject dynamic JSON Schema & Few-Shot Gold Examples, and enforce 2-layer guardrail validation.

**Architecture:** Add `PlatformSchemaReaderPort` and `PlatformExamplesReaderPort` adapters. Create `llm_factory.py`. Refactor `context_node` for schema + gold example injection, and `guardrail_node` for Layer 1 (`jsonschema`) and Layer 2 (compute/PII heuristics) validation.

**Tech Stack:** Python 3.12, LangChain (`init_chat_model`), LangGraph, Pydantic v2, `jsonschema`, Pytest.

## Global Constraints

- Must reside completely inside `services/harness-engine`.
- Maintain backwards compatibility and full test coverage.

---

### Task 1: Agnostic LLM Factory & Configuration

**Files:**
- Modify: `services/harness-engine/src/config.py`
- Create: `services/harness-engine/src/infrastructure/llm_factory.py`
- Create: `services/harness-engine/tests/unit/test_llm_factory.py`

**Interfaces:**
- Consumes: `HarnessSettings` (`LLM_PROVIDER`, `LLM_MODEL`, `LLM_TEMPERATURE`, `LLM_BASE_URL`)
- Produces: `get_llm() -> BaseChatModel`

- [ ] **Step 1: Write the failing test**

```python
# services/harness-engine/tests/unit/test_llm_factory.py
from unittest.mock import patch, MagicMock
from src.infrastructure.llm_factory import get_llm

@patch("src.infrastructure.llm_factory.init_chat_model")
def test_get_llm_calls_init_chat_model_with_config(mock_init: MagicMock) -> None:
    get_llm()
    mock_init.assert_called_once()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd services/harness-engine && uv run pytest tests/unit/test_llm_factory.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Update config and write llm_factory implementation**

In `services/harness-engine/src/config.py`:
```python
    llm_provider: str = "openai"
    llm_model: str = "gpt-4o"
    llm_temperature: float = 0.0
    llm_base_url: str | None = None
```

In `services/harness-engine/src/infrastructure/llm_factory.py`:
```python
from typing import Any
from langchain.chat_models import init_chat_model
from langchain_core.language_models.chat_models import BaseChatModel
from src.config import settings

def get_llm() -> BaseChatModel:
    kwargs: dict[str, Any] = {}
    if settings.llm_base_url:
        kwargs["base_url"] = settings.llm_base_url
    if settings.openai_api_key:
        kwargs["api_key"] = settings.openai_api_key

    return init_chat_model(
        model=settings.llm_model,
        model_provider=settings.llm_provider,
        temperature=settings.llm_temperature,
        **kwargs
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd services/harness-engine && uv run pytest tests/unit/test_llm_factory.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add services/harness-engine/src/config.py services/harness-engine/src/infrastructure/llm_factory.py services/harness-engine/tests/unit/test_llm_factory.py
git commit -m "feat(harness-engine): add agnostic LLM factory with init_chat_model"
```

---

### Task 2: Platform Schema & Gold Examples Reader Ports & Adapters

**Files:**
- Modify: `services/harness-engine/src/domain/ports.py`
- Create: `services/harness-engine/src/infrastructure/adapters/platform_schema_reader.py`
- Create: `services/harness-engine/tests/unit/test_platform_schema_reader.py`

**Interfaces:**
- Consumes: `app.infrastructure.schema_provider` or fallback dicts
- Produces: `PlatformSchemaReaderPort.get_json_schema() -> dict`, `PlatformExamplesReaderPort.get_gold_examples() -> dict[str, str]`

- [ ] **Step 1: Write failing test for PlatformSchemaReader**

```python
# services/harness-engine/tests/unit/test_platform_schema_reader.py
from src.infrastructure.adapters.platform_schema_reader import DefaultPlatformSchemaReader

def test_default_platform_schema_reader_returns_schema() -> None:
    reader = DefaultPlatformSchemaReader()
    schema = reader.get_json_schema()
    assert isinstance(schema, dict)

def test_default_platform_schema_reader_returns_examples() -> None:
    reader = DefaultPlatformSchemaReader()
    examples = reader.get_gold_examples()
    assert "ingestion" in examples
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd services/harness-engine && uv run pytest tests/unit/test_platform_schema_reader.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write Ports and Default Adapter**

Add to `services/harness-engine/src/domain/ports.py`:
```python
class PlatformSchemaReaderPort(ABC):
    @abstractmethod
    def get_json_schema(self) -> dict[str, Any]:
        """Fetch active JSON Schema for pipeline YAML."""
        ...

class PlatformExamplesReaderPort(ABC):
    @abstractmethod
    def get_gold_examples(self) -> dict[str, str]:
        """Fetch canonical Few-Shot Gold YAML examples."""
        ...
```

Create `services/harness-engine/src/infrastructure/adapters/platform_schema_reader.py`:
```python
from typing import Any
from src.domain.ports import PlatformSchemaReaderPort, PlatformExamplesReaderPort
from app.infrastructure.schema_provider import get_pipeline_json_schema, get_gold_examples

class DefaultPlatformSchemaReader(PlatformSchemaReaderPort, PlatformExamplesReaderPort):
    def get_json_schema(self) -> dict[str, Any]:
        return get_pipeline_json_schema()

    def get_gold_examples(self) -> dict[str, str]:
        return get_gold_examples()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd services/harness-engine && uv run pytest tests/unit/test_platform_schema_reader.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add services/harness-engine/src/domain/ports.py services/harness-engine/src/infrastructure/adapters/platform_schema_reader.py services/harness-engine/tests/unit/test_platform_schema_reader.py
git commit -m "feat(harness-engine): add PlatformSchemaReaderPort and DefaultPlatformSchemaReader"
```

---

### Task 3: Refactor Context Node with Schema & Few-Shot Gold Examples Injection

**Files:**
- Modify: `services/harness-engine/src/application/graph/nodes/context_node.py`
- Modify: `services/harness-engine/tests/unit/test_context_node.py` (or workflow tests)

**Interfaces:**
- Consumes: `PlatformSchemaReaderPort`, `PlatformExamplesReaderPort`, `DbSchemaPort`, `StorageMetricsPort`
- Produces: `context_node(state) -> {"context": dict}`

- [ ] **Step 1: Write test for context_node with schema & gold examples**

```python
# Add to tests/unit/test_context_node.py
from src.application.graph.nodes.context_node import make_context_node
from src.infrastructure.adapters.platform_schema_reader import DefaultPlatformSchemaReader

def test_context_node_injects_gold_examples_and_schema() -> None:
    reader = DefaultPlatformSchemaReader()
    node = make_context_node(reader, reader, MagicMock(), MagicMock())
    res = node({"user_prompt": "Ingest sales table", "context": {}})
    ctx = res["context"]
    assert "gold_examples" in ctx
    assert "json_schema" in ctx
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd services/harness-engine && uv run pytest tests/unit/test_context_node.py -v`
Expected: FAIL

- [ ] **Step 3: Update context_node implementation**

In `services/harness-engine/src/application/graph/nodes/context_node.py`:
Update `make_context_node` to accept `schema_reader` and `examples_reader` ports and populate `json_schema` and `gold_examples` in context.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd services/harness-engine && uv run pytest tests/unit/test_context_node.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add services/harness-engine/src/application/graph/nodes/context_node.py tests/unit/test_context_node.py
git commit -m "feat(harness-engine): inject JSON schema and gold examples in context_node"
```

---

### Task 4: Refactor Guardrail Node for 2-Layer Validation (Layer 1 JSON Schema + Layer 2 Intelligence)

**Files:**
- Modify: `services/harness-engine/src/application/graph/nodes/guardrail_node.py`
- Modify: `services/harness-engine/tests/unit/test_guardrail_node.py`

**Interfaces:**
- Consumes: `jsonschema.validate()`, `PipelineSpec`, `state["context"]`
- Produces: `guardrail_node(state) -> {"validation_errors": list[str], "iteration_count": int, "status": str}`

- [ ] **Step 1: Write test for Layer 1 (JSON Schema) validation in guardrail_node**

```python
# In test_guardrail_node.py
def test_guardrail_layer_1_validates_json_schema() -> None:
    # Test that jsonschema validation runs against the platform schema
    pass
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd services/harness-engine && uv run pytest tests/unit/test_guardrail_node.py -v`
Expected: FAIL

- [ ] **Step 3: Update guardrail_node implementation**

Add `jsonschema.validate` check in Layer 1, followed by Layer 2 compute/PII recommendation guardrails.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd services/harness-engine && uv run pytest tests/unit/test_guardrail_node.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add services/harness-engine/src/application/graph/nodes/guardrail_node.py services/harness-engine/tests/unit/test_guardrail_node.py
git commit -m "feat(harness-engine): implement 2-layer guardrail validation"
```

---

### Task 5: Integration & Full Suite Verification

**Files:**
- Modify: `services/harness-engine/src/application/graph/workflow.py`
- Modify: `services/harness-engine/tests/integration/test_workflow.py`
- Modify: `services/harness-engine/tests/integration/test_api.py`

- [ ] **Step 1: Run full test suite**

Run: `cd services/harness-engine && uv run pytest tests/ -v`
Expected: PASS (All 40+ tests passing)

- [ ] **Step 2: Run linters**

Run: `cd services/harness-engine && uv run ruff check src/ tests/ && uv run mypy src/ tests/`
Expected: 0 errors

- [ ] **Step 3: Final Commit**

```bash
git add services/harness-engine/
git commit -m "feat(harness-engine): complete architecture refactoring for 2-layer guardrails and agnostic LLM"
```
