---
name: sdd-ask
description: Classify user intent and route to the correct governed skill pipeline. Single entrypoint for all user requests.
---

## Source

- `.sdd/skills/sdd-ask/skill.yaml`


## Invocation

Use when:

- any user request requiring skill routing

- before any other skill

- operational fix

- diagnostic

- analysis


## Required steps

1. Load `.sdd/agent-instructions.md`
2. Load `.sdd/skills/registry.json`
3. Confirm skill `sdd-ask` is registered
4. Follow the SDD skill protocol
5. Use only allowed CLI

## Allowed CLI


- `sdd ask`

- `sdd governance validate`

- `sdd runtime status --force`


## Risk

`controlled`