---
name: sdd-correct
description: Apply minimal targeted correction to a specific governance violation.
---

## Source

- `.sdd/skills/sdd-correct/skill.yaml`


## Invocation

Use when:

- specific violation identified by sdd-diagnose

- governance drift on a single item


## Required steps

1. Load `.sdd/agent-instructions.md`
2. Load `.sdd/skills/registry.json`
3. Confirm skill `sdd-correct` is registered
4. Follow the SDD skill protocol
5. Use only allowed CLI

## Allowed CLI


- `sdd governance validate`

- `sdd runtime status --force`

- `sdd doctor run`


## Risk

`medium`
