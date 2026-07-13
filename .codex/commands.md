# SDD Commands for Codex

Entrypoint contract:
1. You must learn commands and skills from your custom folder path:
   - `.codex/commands.md`
   - `.codex/skills/`
2. You are under governance. Always resolve instructions from `.sdd`.
   Initial reference: `.sdd/agent-instructions.md`

Use the aliases below as slash commands and route each one to its generated adapter.

## Slash aliases (`/sdd-*`)

| Alias | Adapter target |
|------|----------------|
| `/sdd-pipeline` | `.codex/skills/sdd-pipeline.prompt.md` |
| `/sdd-diagnose` | `.codex/skills/sdd-diagnose.prompt.md` |
| `/sdd-validate-governance` | `.codex/skills/sdd-validate-governance.prompt.md` |
| `/sdd-review-architecture` | `.codex/skills/sdd-review-architecture.prompt.md` |
| `/sdd-harness` | `.codex/skills/sdd-harness.prompt.md` |
| `/sdd-stabilize` | `.codex/skills/sdd-stabilize.prompt.md` |
| `/sdd-compress-context` | `.codex/skills/sdd-compress-context.prompt.md` |
| `/sdd-correct` | `.codex/skills/sdd-correct.prompt.md` |
| `/sdd-converge` | `.codex/skills/sdd-converge.prompt.md` |
| `/sdd-ask` | `.codex/skills/sdd-ask.prompt.md` |
| `/sdd-organize` | `.codex/skills/sdd-organize.prompt.md` |

Notes:
- Canonical commands registry: `.sdd/commands/registry.json`
- Canonical skills registry: `.sdd/skills/registry.json`

SDD GOVERNANCE CHECK
- Always end responses with this compact footer:
  `SDD GOVERNANCE: drift=${status} | governance=${status} | profile=${profile}`

Audit JSON policy:
- `.sdd/compiled/audit/*.json` is human/audit oriented.
- Agents should prefer `.sdd/source/*` for human-readable governance context and
  runtime checks (`sdd runtime status`, `sdd ask --full`) for operational state.
