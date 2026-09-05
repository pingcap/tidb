# Shared-lock upgrade admission — Rust parity receipt

Comparison source: Go `origin/master` at `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`.
The feature authority is commit `94eb995357` (`*: support shared lock upgrade
(`#69559`)`). This batch is Rust-only; no Go source was edited.

## Complete Go inventory

The direct Go variable owner contains 31 tracked artifacts and 18,866 lines at
the comparison revision. The inventory included production, tests, nested test
packages, Bazel targets, and ownership metadata:

```text
BUILD.bazel OWNERS embedding_vars.go embedding_vars_test.go error.go
main_test.go mock_globalaccessor.go mock_globalaccessor_test.go nextgen_test.go
noop.go removed.go removed_test.go sequence_state.go session.go setvar_affect.go
slow_log.go statusvar.go statusvar_test.go sysvar.go sysvar_test.go
tests/BUILD.bazel tests/main_test.go tests/session_test.go
tests/slowlog/BUILD.bazel tests/slowlog/main_test.go tests/slowlog/slow_log_test.go
tests/variable_test.go tidb_vars.go variable.go varsutil.go varsutil_test.go
```

The relevant Go production/test symbols are `vardef.TiDBEnableSharedLockUpgrade`,
`vardef.DefTiDBEnableSharedLockUpgrade`, the GLOBAL|SESSION bool `SysVar`,
`SessionVars.EnableSharedLockUpgrade`, `select.go:newLockCtx`'s assignment to
`LockContext.AllowSharedLockUpgrade`, `TestTiDBEnableSharedLockUpgradeGate`,
and `TestNewLockCtxPropagatesSharedLockUpgrade`. The remaining files were
checked for generated/platform variants, fixtures, package test registration,
and alternate variable setters; none owns another implementation of this gate.

## Rust owner inventory and change

The dependency-closed Rust owner is:

- `third_party/tikv-client-rs/src/kv/types.rs` — source-shaped
  `LockContext.allow_shared_lock_upgrade`, default `false`;
- `third_party/tikv-client-rs/src/transaction/transaction.rs` — exclusive
  lock admission now permits a shared-to-exclusive buffer transition only when
  the context gate is true;
- `crates/tidb-vardef/{src/tidb_vars.rs,src/defaults.rs,src/tests_vardef_port.rs}`
  — constant, default, and generated parity inventory;
- `crates/tidb-session/src/sysvar/catalog/transactions.rs` — registered
  GLOBAL|SESSION bool with Go's OFF default;
- `crates/tidb-session/src/vars.rs` — typed
  `SessionVars::shared_lock_upgrade_enabled`, `SET SESSION`, global seeding,
  and statement snapshot/restore hooks;
- `crates/tidb-session/src/tests_global_vars.rs` — end-to-end session/global
  inheritance regression;
- `crates/tidb-exec/src/mysql_bootstrap/global_variables_fixture.tsv` —
  bootstrap fixture row.

The Rust SQL server currently documents `SELECT ... FOR UPDATE` as an
unported consumer boundary. Therefore this batch closes the lock-context and
session-variable behavior without pretending that the higher-level SQL
executor already constructs a locking context for every statement.

The variable-specific Go Validation closure is also now matched: on the
classic kernel, `ON` and `1` are refused with `ErrWrongValueForVar` (1231) for
both SESSION and GLOBAL writes, while the NextGen build accepts them. The
Rust registry applies this gate after boolean normalization, so the two input
spellings have the same kernel-dependent result as Go.

## Regression evidence

Before the change, the vendored transaction guard rejected every exclusive
request over a shared buffered key whenever `in_share_mode` was false; there
was no context bit or session variable to opt into the Go behavior. The source
regression was added to the existing Go-transcreated `TestLockKeys` carrier and
now proves the enabled path reaches the RPC, changes `SharedLocked` to
`Locked`, and preserves the request's wait time/wake-up mode. The session
regression proves default OFF, session-only isolation, live `@@global` reads,
and reconnect-time inheritance; it also checks the typed Rust session field.

```text
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-session --lib shared_lock_upgrade_variable_has_go_scope_and_default -- --nocapture
# passed: 1 test

cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-session --lib shared_lock_upgrade_switch_uses_go_typed_state -- --nocapture
# passed: 1 test

cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-vardef --lib -- --nocapture
# passed: 43 passed, 107 ignored
```

The standalone vendored client test was attempted. Its package cannot use the
repository lockfile because the nested proto-build workspace pins
`tempfile = 3.14.0` while the locally cached offline index has only newer
compatible versions; a temporary, uncommitted resolver workaround then
exposed pre-existing Tonic 0.14 test-helper API errors (`ProstCodec`,
`empty_body`, and private `BoxBody`). The source-shaped regression remains in
the vendored test carrier, while the dependency-closed workspace check below
compiles the changed client and transaction crates.

## Ready validation

```text
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
# passed

cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-vardef -p tidb-session -p tidb-txnkv
# passed; existing warnings only

cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-session --features tidb-config/nextgen --lib shared_lock_upgrade -- --nocapture
# passed: 2 tests (NextGen acceptance branch)

cargo fmt --manifest-path rust/Cargo.toml --all -- --check
# reports pre-existing formatting drift in unrelated workspace files and
# earlier parity edits; no formatter changes are included in this batch

git diff --check
# passed

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
TMPDIR=/tmp/tidb-codex make lint
# passed (dashboard linter and Go lint targets)
```

The complete `tidb-session --lib` aggregate was also exercised with one test
thread. It reports 1,249 passes, 279 existing planner/storage/fixture
failures, and 208 ignored tests; the focused shared-lock tests and registry
ordering test pass in isolation.

## Boundary and follow-up

The admission bit is deliberately opt-in and defaults OFF, preserving Go's
rollout gate. Classic builds additionally refuse attempts to turn it on,
while NextGen builds retain the enabled path. The remaining parity unit is the
SQL session/executor consumer
that must copy `SessionVars.EnableSharedLockUpgrade` into a `LockContext` for
real locking statements; that owner requires its own complete package audit.
