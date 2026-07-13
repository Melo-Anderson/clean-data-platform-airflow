# ADR 008: Estratégia de Monitoramento e Observabilidade com Prometheus e Probes Dedicados

## Status
Aprovado

## Contexto
A plataforma necessita de uma estratégia robusta de observabilidade e monitoramento para produção que permita:
1. Orquestradores de container (como Kubernetes) avaliarem a saúde e prontidão da API (`liveness` e `readiness` probes).
2. Sistemas de monitoramento (Prometheus) realizarem scraping de métricas técnicas e de domínio.
3. Desacoplamento arquitetural completo entre a infraestrutura de telemetria escolhida (Prometheus) e as camadas lógicas de domínio da aplicação.

## Decisão
Adotou-se a seguinte estratégia:

1. **Desacoplamento de Telemetria (`TelemetryPort`):**
   - Criação de uma porta de aplicação `TelemetryPort` com as assinaturas `record_metric` e `record_event`.
   - Implementação de um adaptador de infraestrutura `PrometheusMetricsAdapter` usando a biblioteca `prometheus-client`.
   - Injeção da interface `TelemetryPort` nas camadas que precisam disparar telemetria (ex: middleware de HTTP e casos de uso de pipeline).

2. **Endpoints de Integridade (Health Probes):**
   - **Liveness probe (`GET /health`)**: Focado exclusivamente na vivacidade da aplicação (sem checagem de I/O em banco ou Vault) para prevenir falhas em cascata induzidas por timeouts em serviços de dependência.
   - **Readiness probe (`GET /health/ready`)**: Realiza consultas ativas reais (`SELECT 1` no PostgreSQL e `/v1/sys/health` no OpenBao) para expor a prontidão antes do tráfego ser roteado para o pod/container.

3. **Métricas Padronizadas expostas em `/metrics`:**
   - Exposição via FastAPI usando codificação e tipo de mídia oficiais do Prometheus.
   - Registro isolado em testes unitários para evitar colisões na inicialização paralela de testes de escopo global.

## Consequências
- **Positivas**:
  - Camada de domínio livre de acoplamento direto com código do Prometheus.
  - Mitigação de falhas em cascata de infraestrutura em orquestradores (K8s).
  - Facilidade de teste das chamadas de métricas via mocks.
- **Negativas**:
  - Aumenta levemente a complexidade do middleware HTTP por conta de cálculo de latência e concorrência no isolamento do coletor em testes unitários.
