# domain sysvar-cache parity audit: Go `pkg/domain/sysvar_cache.go` vs `sysvar_cache.rs`

Audit date: 2026-09-05. Opening slice of the domain-surface audit.

## Inventory

All thirteen non-test Go files of `pkg/domain` have mirrored Rust
modules in `tidb-domain/src` (`domain_sysvars`, `sysvar_cache`,
`schema_checker`, `serverinfo_syncer`, `topn_slow_query`,
`historical_stats`, `ru_stats`, `plan_replayer`, `optimize_trace`,
`disttask`, `domainutil`, `cdcutil`, `replayer`), plus
`status_endpoint_claim` for the fork's own extension.

## sysvar_cache slice: VERIFIED

All six Go functions exist with their semantics: the empty-cache
rebuild trigger, the session-cache clone, the global lookup with
`ErrUnknownSystemVar` for a name outside the global view (including a
variable that has no global scope, as Go reports), `fetchTableValues`
over `mysql.global_variables`, the starter-mode
`max_allowed_packet` config override, and the rebuild that writes the
global view BEFORE running `SetGlobal` so a clamping hook answers the
unclamped string — the ordering subtlety is documented in both the
module doc and the code. Traits abstract the session pool and
restricted-SQL executor so the cache is testable without a cluster.

The remaining domain files are inventory-verified only; per-module
behavioral audits follow the same pattern as this slice.
