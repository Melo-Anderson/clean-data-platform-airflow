# ADR 007: Resiliência em Integrações de API e Repositórios usando Circuit Breakers

## Status
Aprovado

## Contexto
A plataforma faz chamadas externas essenciais a outros serviços (como a API do Airflow, OpenBao e bancos de dados transacionais). Quando uma dessas dependências externas apresenta instabilidades, lentidão ou indisponibilidade, tentativas sucessivas e síncronas de reconexão podem:
1. Exaurir threads e recursos locais de processamento da API (ex: pool de conexões HTTP, conexões de banco de dados).
2. Piorar o estado do serviço de destino sobrecarregando-o ainda mais.
3. Gerar latência excessiva para os usuários finais antes de falhar de fato.

## Decisão
Implementou-se um padrão de **Circuit Breaker** customizado na camada de infraestrutura/comunicação da aplicação:
1. **Transição de Estados:**
   - **CLOSED**: Estado normal. As chamadas fluem para o destino. Erros sucessivos incrementam um contador.
   - **OPEN**: Se a taxa de falha atingir o limite (threshold) configurado em uma janela de tempo, o circuito abre. Todas as novas chamadas falham imediatamente com `CircuitBreakerOpenError`, poupando recursos.
   - **HALF-OPEN**: Após um período de resfriamento (cooldown), o circuito permite chamadas de teste (probes). Se forem bem-sucedidas, o circuito volta para `CLOSED`. Se falharem, retorna para `OPEN`.
2. **Desacoplamento nos Adaptadores:**
   - O Circuit Breaker é encapsulado nos adaptadores de repositório e comunicação (como o `AirflowOrchestratorAdapter`), garantindo que a lógica de aplicação e os casos de uso não precisem gerenciar estados de falha de conexão de baixo nível.

## Consequências
- **Positivas**:
  - Maior robustez e resiliência contra falhas transientes e falhas parciais em cascata.
  - Fail-fast imediato quando dependências externas comprovadamente falharam.
- **Negativas**:
  - Introduz complexidade adicional de gerenciamento de estado assíncrono (e concorrência de threads) nos adaptadores da infraestrutura.
  - Exige monitoramento e alertas adicionais para informar aos operadores quando um circuito for aberto.
