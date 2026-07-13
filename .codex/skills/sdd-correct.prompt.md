---
description: sdd-correct command adapter
mode: agent
---

## Source

`.sdd/commands/sdd-correct/command.yaml`

## Required behavior

1. Run preflight: `sdd runtime status`
2. Validate governance: `sdd governance validate`
3. Execute:

   `sdd skills run sdd-correct`

4. Return `policy_result` and `next_actions`

## SDD GOVERNANCE

`SDD GOVERNANCE: drift=${status} | governance=${status} | profile=sdd-correct`
