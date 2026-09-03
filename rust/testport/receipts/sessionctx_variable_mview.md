# `pkg/sessionctx/variable` + `pkg/sessionctx/vardef` — materialized-view session-variable drift receipt (Go-master parity)

Comparison source: Go `origin/master` at commit
`94a9cbedabbb3190fd892a196dd446df48b7ec6e` (2026-09-03). Batch scope: the
sessionctx slice of Go's materialized-view DDL commit — the exact drift
`git show 94a9cbedab -- pkg/sessionctx/vardef pkg/sessionctx/variable`
(5 files, +365/−24); prior area pins (`sessionctx_variable.md`,
`sessionctx_vardef_audit.md`, the b-series vardef receipts) are unchanged
except where this commit touches them.

## Drift content and Rust alignment

- `vardef/tidb_vars.go`: five new variable names (`TiDBMViewMaintainMemQuota`,
  `TiDBMViewMaintainIsolationReadEngines`, `TiDBMViewMaintainImportThreads`,
  `TiDBMViewMaintainImportDiskQuota`, `TiDBMViewEnable`) and
  `MaxConfigurableConcurrency` → `tidb-vardef` `tidb_vars.rs` constants at
  their Go positions. (`MaxConfigurableConcurrency = 256` predates the commit
  but was absent from the Rust owner; this batch consumes it.)
- `vardef/tidb_vars.go` defaults: `DefTiDBMViewMaintainMemQuota = 2GB`,
  `DefTiDBMViewMaintainImportThreads = 0`,
  `DefTiDBMViewMaintainImportDiskQuota = ""`, `DefTiDBMViewEnable = false` →
  `defaults.rs`.
- `variable/session.go`: `SessionVars.MViewMaintainMemQuota`,
  `MViewMaintainIsolationReadEngines`, `MViewMaintainImportThreads`,
  `MViewMaintainImportDiskQuota`, `EnableMView`, `InMViewMaintenance` and the
  `NewSessionVars` initial values. The Rust `SessionVars` carrier stores
  sysvar text with typed fields only for hook-driven hot state (documented
  crate policy), so the first five surface through `get_system` against the
  new registry entries (defaults included) and `InMViewMaintenance` —
  programmatic, never a sysvar — is a typed field with accessors.
- `variable/sysvar.go`: five `defaultSysVars` entries (bool enable;
  mem-quota int with Go's `<128 → 128` truncated-value clamp and `-1`
  minimum; isolation-read-engines string with the shared normalizer;
  import-threads int bounded by `MaxConfigurableConcurrency`;
  disk-quota string with the go-units size check) → `sysvar/catalog/mview.rs`
  entries; `normalizeIsolationReadEnginesValue` +
  `defaultIsolationReadEnginesValue` → a shared
  `normalize_isolation_read_engines_value` helper wired into
  `SysVarDef::run_validation` for BOTH `tidb_isolation_read_engines` (closing
  this crate's pre-existing validation gap for that variable — Go's SET
  canonicalizes case and refuses empty/unknown engines) and the new
  maintained-engines variable; the mem-quota clamp returns
  `Validated::truncated`, which renders Go's `ErrTruncatedWrongValue` warning
  on the SET path; the disk-quota check reuses `tidb-config`'s
  `ram_in_bytes` (now `pub`) with `ErrWrongValueForVar` on zero/unparsable.
- `variable/variable.go`: `MViewExecutionSessionVars`,
  `MViewExecutionSessionVarsApplyConfig`,
  `CaptureMViewExecutionSessionVars`,
  `CaptureAppliedMViewExecutionSessionVars`, `GetIsolationReadEnginesString`,
  `ApplyMViewExecutionSessionVarsWithConfig`,
  `buildMViewExecutionSessionVarAssignments` (all twelve Go assignments in
  order, with Go's failure messages) and `restoreMViewExecutionSessionVars`
  → `tidb-session` `vars.rs`. Go's returned restore closure becomes a
  `MViewExecutionVarsRestore` handle whose `restore(&mut SessionVars)` runs
  the captured assignments (Rust cannot re-borrow the session inside a
  returned closure); the config's callback fields become boxed callbacks and
  callback error payloads carry Go's variable message text. Capture reads the
  validated session text and parses it, which is value-identical to Go's
  hook-maintained typed fields.

## Known gaps recorded (not fixed in this batch)

- Go's `SetSession`/`GetSession` closures for the new variables (typed
  `EnableMView`/`MViewMaintain*` field maintenance) follow the crate's
  documented "hooks not modelled" policy; consumers read the validated text.
- Go's mem-quota validation warning rides `vars.StmtCtx.AppendWarning`
  directly; the Rust apply machinery surfaces the clamp as the `set_system`
  truncated flag (rendered by the SET path) and does not append to a
  statement context it does not own.
- Go's limits block beyond `MaxConfigurableConcurrency` (e.g.
  `MaxShardRowIDBits`) is still absent from the Rust `tidb-vardef` owner —
  pre-existing drift outside this batch.

## Regression tests

`rust/crates/tidb-session/src/tests_mview_session_vars.rs` (11 running
tests): `mview_enable_roundtrip` (Go `TestTiDBMViewEnable`),
`mview_maintain_mem_quota_clamps_below_128`,
`isolation_read_engines_normalize_is_shared` (both variables, canonical case,
empty/unknown refusals), `mview_maintain_import_disk_quota_validation`,
`mview_maintain_import_threads_bounds`, `capture_on_fresh_session_reads_defaults`,
`apply_and_restore_mview_execution_vars` (apply, capture-applied, restore),
`apply_mview_is_noop_when_origin_equals_target`,
`apply_mview_best_effort_reports_and_continues`,
`apply_mview_strict_fails_and_restores` (Go's annotated failure text and
already-applied rollback), `in_mview_maintenance_flag_roundtrip`. The
`tidb-vardef` pin table `SYSVAR_NAME_CONSTANTS` gains the five name pairs and
`sysvar.rs::the_registry_is_complete_and_sorted` pins the aligned registry
count (965 + Go's five).

Fail-before evidence: with the three new `run_validation` cases reverted, the
normalize/clamp/disk-quota tests fail; the machinery tests bind to symbols
that do not compile against the pre-batch tree.

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-vardef -p tidb-config --no-fail-fast
# 143/143 passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-session -E 'test(mview)' --no-fail-fast
# 11/11 passed (new regressions)
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-session --no-fail-fast
# failure set IDENTICAL to the pre-batch base commit `06bccf90e2`
# (690 documented pre-existing baseline failures; zero new, zero fixed)
cargo +nightly-2026-08-22 check --offline --locked -p tidb-session -p tidb-config
# 0 errors
```

No Go source changed in this batch.
