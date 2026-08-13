# ADR 014: Geração Declarativa de Pipelines via LangGraph e Guardrails HTTP

## Status
Aprovado

## Contexto
A geração autônoma de especificações de pipeline a partir de prompts em linguagem natural utilizando LLMs apresenta riscos de alucinação de sintaxe, tipos inválidos e incompatibilidades com os esquemas da plataforma.

## Decisão
Adotou-se o motor **Harness Engine** baseado em **LangGraph** e **Clean Architecture**:

1. **Grafo de Estados Determinístico (`LangGraph`):**
   - `context_node`: Consulta metadados reais da plataforma e exemplos Gold.
   - `planner_node`: Decompõe a intenção do usuário em um plano de execução.
   - `generator_node`: Gera especificações Pydantic (`PipelineSpec`) via Structured Output.
   - `guardrail_node`: Submete o YAML gerado para validação HTTP na própria API (`POST /v1/harness/validate`).
   - `enricher_node`: Reinjeta mensagens de erro sintático/semântico no prompt para autocorreção em iterações subsequentes.
   - `hitl_node`: Permite revisão humana (*Human-in-the-Loop*) antes da persistência final.

## Consequências
- **Positivas**:
  - Garantia de especificações YAML 100% válidas e executáveis.
  - Autocorreção autônoma de erros de geração sem intervenção humana prematura.
- **Negativas**:
  - Latência adicional de iterações no grafo em casos onde o LLM inicial necessita de correções.
