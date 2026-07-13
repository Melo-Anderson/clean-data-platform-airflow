---
description: sdd-compress-context command adapter
---

Source: `.sdd/commands/sdd-compress-context/command.yaml`

Steps:
1. Read `.sdd/agent-instructions.md`
2. Run `sdd runtime status`
3. Run `sdd governance validate`
4. Execute:

   `sdd skills run sdd-compress-context`

5. Return `policy_result` and `next_actions`