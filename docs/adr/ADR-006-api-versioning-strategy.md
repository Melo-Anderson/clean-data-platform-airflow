# ADR 006: Estratégia de Versionamento de API

## Status
Aprovado
**Data:** 2026-07-10

## Contexto
A plataforma expõe endpoints REST para operações de metadados, pipelines, ativos de dados e orquestração. Para garantir compatibilidade com versões antigas (retrocompatibilidade) e possibilitar a evolução de contratos de dados sem quebrar consumidores existentes, precisamos de uma estratégia clara de versionamento da API HTTP.

## Alternativas Consideradas

| Alternativa | Prós | Contras |
|-------------|------|---------|
| **Versionamento por Header (`Accept: application/vnd.platform.v1+json`)** | URLs limpas, versionamento fino | Difícil de testar/debugar no browser, cacheamento HTTP complexo |
| **Sem Versionamento (Contratos Flexíveis)** | Simplicidade extrema | Risco iminente de quebra de integração em produção a cada deploy |
| **Versionamento por Prefixo de URL (`/v1/`)** | Visibilidade clara, fácil de depurar e testar, roteamento simples | Altera a URL raiz para todos os clientes |

## Decisão
Adotar o **versionamento por prefixo de URL (`/v1/`)** de forma global no FastAPI. Todos os roteadores de recursos (`assets`, `endpoints`, `pipelines`, `discovery`, `lineage`) serão inclusos sob o prefixo `/v1/`.

## Consequências
- ✅ Isolamento absoluto de quebras de contratos futuros (novos endpoints em `/v2/` podem coexistir)
- ✅ Suporte claro e transparente a testes end-to-end e ferramentas de debug HTTP (como Swagger/OpenAPI)
- ⚠️ Necessidade de atualização de todas as chamadas relativas em suítes de teste (e.g. `tests/`)
- ⚠️ Alembic e DB Migrations devem gerenciar compatibilidade caso o schema do banco precise mudar de forma não retrocompatível (migrations não reversíveis/sem down migrations devem ser documentadas detalhadamente).
