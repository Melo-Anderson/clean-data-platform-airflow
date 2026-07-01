---
name: strategist
description: Invoca a orquestração do Strategist quando o usuário digita /strategist
---
# Strategist Skill

Sempre que o usuário digitar `/strategist` ou solicitar a orquestração do Strategist, você deve iniciar uma missão de orquestração do Strategist:
1. Leia o arquivo `.strategist/active.yaml` e os contratos em `.strategist/contracts/`.
2. Execute o pipeline nas fases: bootstrap, intake, discovery, refinement, approval_gate, execution, adr, learning.
3. Siga estritamente as regras de personas e de compliance especificadas no protocolo.
