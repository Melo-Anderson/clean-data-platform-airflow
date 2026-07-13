# SDD CLI Commands for Gemini

Use these commands when asked to run tests, lint, or manage governance:

| Task | Command |
|------|---------|
| Run tests | `sdd test run` |
| Lint | `sdd lint run` |
| Validate governance | `sdd governance validate` |
| Compile governance | `sdd governance compile` |
| Runtime status | `sdd runtime status` |
| Query context | `sdd ask --full "<question>"` |
| Organize large context | `sdd organize "<context>"` |
| Diagnostics | `sdd doctor run --mode real` |
| Generate agent seeds | `sdd governance generate` |

Exit codes for `sdd runtime status`: 0=HEALTHY, 1=NOT_INITIALIZED, 2=MISCONFIGURED, 3=NOT_CONNECTED.

SDD GOVERNANCE CHECK
- Always end responses with this compact footer:
  `SDD GOVERNANCE: drift=${status} | governance=${status} | profile=${profile}`

Audit JSON policy:
- `.sdd/compiled/audit/*.json` is human/audit oriented.
- Agents should prefer `.sdd/source/*` for human-readable governance context and
  runtime checks (`sdd runtime status`, `sdd ask --full`) for operational state.
