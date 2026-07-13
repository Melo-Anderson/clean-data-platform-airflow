---
name: sdd-diagnose
description: Diagnose runtime/workspace problems with governed checks.
---

## Source

- `.sdd/skills/sdd-diagnose/skill.yaml`


## Invocation

Use when:

- failing checks

- unknown workspace failures


## Required steps

1. Load `.sdd/agent-instructions.md`
2. Load `.sdd/skills/registry.json`
3. Confirm skill `sdd-diagnose` is registered
4. Follow the SDD skill protocol
5. Use only allowed CLI

## Allowed CLI


- `sdd doctor run`

- `sdd runtime status --force`


## Risk

`low`
