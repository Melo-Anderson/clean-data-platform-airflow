---
description: sdd-harness command adapter
---

Source: `.sdd/commands/sdd-harness/command.yaml`

Steps:
1. Read `.sdd/agent-instructions.md`
2. Run `sdd runtime status`
3. Run `sdd governance validate`
4. Execute:

   `sdd skills run sdd-harness`

5. Return `policy_result` and `next_actions`
