---
name: sdd-validate-governance
description: Validate governance integrity and runtime preflight.
---

## Source

- `.sdd/skills/sdd-validate-governance/skill.yaml`


## Invocation

Use when:

- pre-delivery gate

- compliance checks


## Required steps

1. Load `.sdd/agent-instructions.md`
2. Load `.sdd/skills/registry.json`
3. Confirm skill `sdd-validate-governance` is registered
4. Follow the SDD skill protocol
5. Use only allowed CLI

## Allowed CLI


- `sdd governance validate`

- `sdd runtime status`


## Risk

`medium`
