# External Microservices

The AI Harness Engine previously located in `services/harness-engine` has been migrated into its own dedicated repository:

- **Repository:** `pipeline-harness-ai`
- **Responsibility:** AI-powered YAML Pipeline Specification Generation & 2-Layer Guardrail Validation Engine.
- **Communication:** Communicates with this Platform via HTTP APIs (`/v1/harness/schema`, `/v1/harness/gold-examples`).
