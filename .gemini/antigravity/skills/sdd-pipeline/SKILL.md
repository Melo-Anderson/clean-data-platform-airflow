---
name: sdd-pipeline
description: Orchestrate ask -> diagnose -> correct -> converge as a governed skill pipeline.
---

## Source

- `.sdd/skills/sdd-pipeline/skill.yaml`


## Invocation

Use when:

- strict end-to-end correction flow

- pipeline orchestration

- multi-stage governed remediation


## Required steps

1. Load `.sdd/agent-instructions.md`
2. Load `.sdd/skills/registry.json`
3. Confirm skill `sdd-pipeline` is registered
4. Follow the SDD skill protocol
5. Use only allowed CLI

## Allowed CLI


- `sdd ask`

- `sdd skills run sdd-diagnose`

- `sdd skills run sdd-correct`

- `sdd skills run sdd-converge`


## Risk

`controlled`
