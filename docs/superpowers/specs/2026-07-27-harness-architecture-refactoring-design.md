# Harness Engine Architecture Refactoring Design Spec

**Date:** 2026-07-27
**Status:** Draft / Under Review
**Target Component:** `services/harness-engine`

---

## 1. Overview & Objectives

The `harness-engine` service generates and validates data pipeline YAML specifications using an AI-driven agentic workflow built on LangGraph. Following initial implementation, two critical maintenance challenges were identified:

1. **Governance & Syntax Drift:** Business rules and valid YAML parameters were static/hardcoded inside the Harness Engine, creating a risk of divergence whenever the core Data Platform evolves its schema or parameters.
2. **LLM Provider Coupling:** Direct dependency on `ChatOpenAI` restricted flexibility to switch or test alternative LLM providers (e.g., Anthropic Claude, Google Gemini, Ollama/vLLM, Azure OpenAI).

### Goals
- **Decouple Syntax Contract:** Fetch dynamic YAML parameter contracts (JSON Schema) and canonical "Gold Examples" directly from the Data Platform.
- **2-Layer Guardrail Architecture:** Separate structural syntax validation (Platform responsibility) from performance/governance recommendation logic (Harness Engine intelligence).
- **Agnostic LLM Provider:** Refactor model instantiation using LangChain's `init_chat_model` for zero-code provider switching via environment variables.

---

## 2. Architecture & Component Design

```
                     ┌──────────────────────────────────────────┐
                     │          Data Platform API/DB            │
                     └────────────────────┬─────────────────────┘
                                          │
                     ┌────────────────────┴─────────────────────┐
                     │          Platform Integration            │
                     │  - PlatformSchemaReaderPort (JSON Schema)│
                     │  - PlatformExamplesReaderPort (Gold YAMLs)│
                     └────────────────────┬─────────────────────┘
                                          │
                                          ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                 Harness Engine LangGraph                                │
│                                                                                         │
│  ┌──────────────────────┐      ┌──────────────────────┐      ┌───────────────────────┐  │
│  │     context_node     │ ───► │    generator_node    │ ───► │    guardrail_node    │  │
│  │                      │      │                      │      │                       │  │
│  │ Injects JSON Schema  │      │ Agnostic LLM Factory │      │ Layer 1: JSON Schema  │  │
│  │ + Few-Shot Gold YAMLs│      │ (init_chat_model)    │      │ Layer 2: Compute/PII  │  │
│  │ + Telemetry/Metadata │      │                      │      │          Rules        │  │
│  └──────────────────────┘      └──────────────────────┘      └───────────┬───────────┘  │
│                                                                          │              │
│                                           ┌──────────────────────────────┴───────────┐  │
│                                           │             Routing Edge                 │  │
│                                           │ (Approved -> End / Errors -> Retry Loop) │  │
│                                           └──────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### 2.1 Component Responsibilities

#### A. Platform Syntax Contract & Few-Shot Reader Ports (`src/domain/ports.py`)
- **`PlatformSchemaReaderPort`**: Abstract port to retrieve the active JSON Schema defining valid keys, types, and enums for pipeline YAMLs.
- **`PlatformExamplesReaderPort`**: Abstract port to retrieve canonical, production-tested "Gold YAML" examples for ingestion, ETL, and export pipelines.

#### B. Context Node Enhancement (`src/application/graph/nodes/context_node.py`)
- Injects the active **JSON Schema contract** into the System Prompt.
- Injects **Few-Shot Gold Examples** corresponding to the requested pipeline type to guide LLM output format and eliminate syntax hallucinations.
- Injects historical telemetry and schema metadata (volume, duration, PII column names).

#### C. Agnostic LLM Factory (`src/infrastructure/llm_factory.py`)
- Replaces direct `ChatOpenAI` instantiation with LangChain's `init_chat_model(...)`.
- Resolves provider, model name, temperature, and optional `base_url` dynamically from settings.

#### D. Two-Layer Guardrail Node (`src/application/graph/nodes/guardrail_node.py`)
1. **Layer 1: Structural Contract Validation (Platform Schema):**
   - Validates generated YAML against the fetched JSON Schema using `jsonschema.validate()`.
   - Catches unknown keys, invalid enums, and missing required parameters.
2. **Layer 2: Harness Intelligence & Optimization (Compute & Governance):**
   - Evaluates telemetry metrics (data volume, average duration) to recommend compute sizing (e.g., forcing `spark` engine for volumes $>100\text{GB}$, scaling worker count).
   - Evaluates DB metadata to enforce governance rules (masking PII columns, attaching required quality rules).

---

## 3. Configuration & Environment Variables

Refactor `HarnessSettings` in `src/config.py`:

| Variable | Type | Default | Description |
|---|---|---|---|
| `LLM_PROVIDER` | `str` | `openai` | Model provider (`openai`, `anthropic`, `google_genai`, `ollama`, `azure_openai`) |
| `LLM_MODEL` | `str` | `gpt-4o` | Model identifier (e.g., `gpt-4o`, `claude-3-5-sonnet-20241022`, `llama3`) |
| `LLM_TEMPERATURE` | `float` | `0.0` | Sampling temperature |
| `LLM_BASE_URL` | `str \| None` | `None` | Optional base URL for local models (Ollama/vLLM) or proxies |
| `PLATFORM_SCHEMA_URL` | `str \| None` | `None` | Endpoint or path to fetch dynamic JSON Schema |

---

## 4. Verification & Testing Strategy

1. **Unit Tests:**
   - `test_llm_factory.py`: Test model instantiation across mock configuration settings (`openai`, `anthropic`, `ollama`).
   - `test_guardrail_node.py`: Test Layer 1 (JSON Schema validation failures) and Layer 2 (compute optimization and PII guardrails) independently.
   - `test_platform_schema_reader.py`: Test schema fetching and fallback mechanisms.
2. **Integration Tests:**
   - `test_workflow.py`: Validate the full LangGraph flow with JSON Schema context injection and LLM retry feedback loop when Layer 1 or Layer 2 validation fails.
