# `pkg/sessionctx/stmtctx` — complete Go-master parity boundary receipt

## Follow-up batch — alternative-plan storage signals

Comparison source: Go `origin/master` at commit
`1c1a334d2be1dce64888b6e1f054462c566b0734`.
The complete four-artifact inventory below remains the package boundary: all
2,416 lines of production code, tests, harness, and BUILD metadata were read
before editing; there are no fixtures, generated outputs, or platform variants.

Go master adds three statement-local alternative-plan signals and their mark
helpers: a mixed TiKV/TiFlash plan, a missing TiFlash path, and an explicit
`READ_FROM_STORAGE` preference. The reset path clears all three. The Go fields,
helpers, and regression test were restored. Rust's dependency-closed
`tidb-exec::AlternativePlanSignals` owner now carries the same three fields and
transitions, and its source regression covers mark and reset behavior for all
eleven signals.

This is a bounded package behavior batch, not a package-complete
transcreation claim. The broader StatementContext integration and alternative
round driver remain explicit boundaries.

## Complete inventory

The package contains four tracked artifacts and 2,416 lines. Every production
method, constructor, reset path, warning/error helper, planner build-state
carrier, statement-cache seam, tracker/status hook, test, benchmark, TestMain,
and the 17-shard flaky Bazel target was read before this receipt was written.
There is no `doc.go`, fixture or `testdata` directory, generated output,
platform-specific variant, fuzz target, or generator input.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 67 | `58c2bbd7a4110509889d5e4c2480e029cf094d8e` | `9ca7b75733b7c3bfff6ca9c5b677940057bd3fbb44258b27eb2cee889cc089b9` | stmtctx library and 17-shard flaky test target |
| `main_test.go` | 34 | `f34da9ae0cc07f73d02b9fd6fa0a92c301eeaa64` | `d1b05c65bfb2fd497021c29517bca2470cc856e9e9a0c6b48df151419ba8e2d3` | TestMain/goleak and failpoint test harness |
| `stmtctx.go` | 1,674 | `42982b6842ed11764d3e61e9191781f5fd5f43fa` | `f9557e8503fe0dae4f2977f0ab0e9c3aea464ff499be763a52fe3ccb8f5508d8` | StatementContext lifecycle, flags, warnings, caches, trackers, status, and planner state |
| `stmtctx_test.go` | 641 | `e6115eaa339c5e0e507346dbf376467d291a72a2` | `707800caf572d4277bd7b338015ab59c40c41aa25b65c19e709de7386f58676c` | focused statement-context tests and `BenchmarkErrCtx` |

`stmtctx.go` declares 129 production functions (including constructors,
methods, and helpers). The tests contain 17 `Test*` functions, one benchmark,
and the package `TestMain`: `TestCopTasksDetails`,
`TestStatementContextPushDownFLags`, `TestWeakConsistencyRead`,
`TestMarshalSQLWarn`, `TestLogicalPlanBuildStateRestore`,
`TestQBHintHandlerBuildState`, `TestApproxRuntimeInfo`, `TestStmtHintsClone`,
`TestNewStmtCtx`, `TestSetStmtCtxTimeZone`, `TestSetStmtCtxTypeFlags`,
`TestResetStmtCtx`, `TestStmtCtxID`, `TestIssue58600`, `TestErrCtx`,
`TestReservedRowIDAlloc`, and `TestUsedStatsInfoForTableWriteToSlowLog`.

The Go production and test files changed in this batch only for the three
alternative-plan storage signals and their reset regression. The remaining
StatementContext behavior is unchanged from the comparison source and remains
outside this bounded fix.

## Rust ownership and explicit boundary

Rust has executable owners for subsets of this contract in
`rust/crates/tidb-executor/src/stmt_context.rs`,
`rust/crates/tidb-session/src/stmt_ctx.rs`, and the `tidb-exec` statement
context/result/status/cache modules. Source-derived Rust tests cover selected
pushdown flags, stale TSO, statement-cache behavior, status/error conversion,
reserved row IDs, used statistics, and alternative-plan signals. These owners
are split across executor, session, planner, error, type, and metrics seams;
there is no dependency-closed Rust owner for the complete Go `StatementContext`
package, its TestKit/Domain integration, and all 17 tests. Existing ignored
carriers therefore remain explicit seed evidence rather than a transcreated
package. No Rust-only behavior was found to remove, and no safe standalone
implementation can be added without duplicating or changing those owners.

## Follow-up validation

- The focused Go failpoint-aware test
  `TestAlternativeLogicalPlanStorageSignalsReset` passes, as does the full
  failpoint-aware `pkg/sessionctx/stmtctx` suite (`PASS`, 2.648s).
- The Rust source-backed target
  `cargo +nightly-2026-08-22 test --offline --locked -p tidb-exec --test all alternative_plan_signal -- --test-threads=1`
  was attempted but is blocked before test execution by the local
  `openssl-sys`/`pkg-config` dependency (`pkg-config` and OpenSSL headers are
  unavailable).
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed as the Ready gate.
- `make bazel_prepare` is required because the Go production and test files
  changed; it remains blocked locally because no `bazel` executable is
  installed.

## Validation and risk

Profile: **Ready** for this bounded code batch. The package regression and
repository lint gate were run; package-complete parity is not claimed because
the broader StatementContext integration remains an explicit boundary.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/sessionctx/stmtctx -count=1
# PASS ok github.com/pingcap/tidb/pkg/sessionctx/stmtctx 2.811s
```

The wrapper enabled failpoints before the exact Go-master suite and disabled
them during teardown. `make bazel_prepare` was required by the Go source/test
changes but is blocked by the missing Bazel executable. Not verified: the
dependency-closed Rust target (blocked by OpenSSL/pkg-config), full Bazel shard
execution, or the broader repository Ready profile. Correctness risk is low
for the statement-local boolean state and reset path; compatibility risk is
limited to consumers that begin reading these restored signals, and no
performance-sensitive loop was changed.

This receipt certifies the bounded `pkg/sessionctx/stmtctx` inventory and
ownership boundary; it is not a repository-wide parity claim.
