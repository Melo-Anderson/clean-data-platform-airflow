---
name: sdd-harness
description: Bootstrap pointer skill for file-based skill-discovery agents (Claude Code, Antigravity/Gemini CLI). Points to .sdd/agent-instructions.md as the single governance authority.
---

## Source

- `.sdd/skills/sdd-harness/skill.yaml`


## Invocation

Use when:

- agent startup / skill discovery

- locating canonical governance entrypoints


## Required steps

1. Load `.sdd/agent-instructions.md`
2. Load `.sdd/skills/registry.json`
3. Confirm skill `sdd-harness` is registered
4. Follow the SDD skill protocol
5. Use only allowed CLI

## Allowed CLI


- `sdd runtime status`


## Risk

`low`
