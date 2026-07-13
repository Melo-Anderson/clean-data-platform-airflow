---
description: sdd-organize command adapter
mode: agent
---

## Source

`.sdd/commands/sdd-organize/command.yaml`

## Required behavior

1. Run preflight: `sdd runtime status`
2. Validate governance: `sdd governance validate`
3. Execute:

   `sdd organize`

4. Return `policy_result` and `next_actions`

## SDD GOVERNANCE

`SDD GOVERNANCE: drift=${status} | governance=${status} | profile=sdd-organize`