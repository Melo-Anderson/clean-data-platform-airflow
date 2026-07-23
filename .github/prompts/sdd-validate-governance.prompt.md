---
description: sdd-validate-governance command adapter
---

Source: `.sdd/commands/sdd-validate-governance/command.yaml`

Steps:
1. Read `.sdd/agent-instructions.md`
2. Run `sdd runtime status`
3. Run `sdd governance validate`
4. Execute:

   `sdd skills run sdd-validate-governance`

5. Return `policy_result` and `next_actions`
