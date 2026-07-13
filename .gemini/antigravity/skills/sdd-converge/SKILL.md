---
name: sdd-converge
description: Drive systemic alignment toward spec target after targeted corrections.
---

## Source

- `.sdd/skills/sdd-converge/skill.yaml`


## Invocation

Use when:

- residual delta after sdd-correct passes

- pattern of drift detected


## Required steps

1. Load `.sdd/agent-instructions.md`
2. Load `.sdd/skills/registry.json`
3. Confirm skill `sdd-converge` is registered
4. Follow the SDD skill protocol
5. Use only allowed CLI

## Allowed CLI


- `sdd ask`

- `sdd governance validate`

- `sdd runtime status --force`

- `sdd doctor run`


## Risk

`high`