# sirop Logging Spec — Per-Module Guidelines

Reference for what each pipeline module should log at each level.
See `CLAUDE.md §User Output — Two Channels` for the emit() / logger split,
log level rules, and privacy/redaction details.

## Per-Module Log Guidelines

| Module | INFO | WARNING | DEBUG |
|--------|------|---------|-------|
| Importers | _(none — tap result goes via emit())_ | Missing fields, unknown tx types | Each parsed row |
| Normalizer | `"Checking sap levels..."` (BoC fetch) | Missing rates, unusual types | Each BoC rate, each field converted |
| Node verifier | `"Verifying on-chain..."` | `"Node unreachable — running without on-chain verification."`, discrepancies | Raw API fields, before/after overrides |
| Transfer matcher | `"Tracing the flow..."`, row counts | Unmatched withdrawals/deposits; same-wallet transfer (coin consolidation) | Window checks |
| ACB engine (PURE) | — | — | ACB state before/after each event |
| SLD engine (PURE) | — | — | Each 61-day window check |
| Repository | — | Schema version mismatch | Every SQL query |
