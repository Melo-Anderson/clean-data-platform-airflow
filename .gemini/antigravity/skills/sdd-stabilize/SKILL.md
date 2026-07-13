---
name: sdd-stabilize
description: Run stabilization checks before handoff.
---

## Source

- `.sdd/skills/sdd-stabilize/skill.yaml`


## Invocation

Use when:

- before release

- before merge


## Required steps

1. Load `.sdd/agent-instructions.md`
2. Load `.sdd/skills/registry.json`
3. Confirm skill `sdd-stabilize` is registered
4. Follow the SDD skill protocol
5. Use only allowed CLI

## Allowed CLI


- `sdd lint run`

- `sdd test ci-validate`


## Risk

`medium`
