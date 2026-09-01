# `pkg/sessionctx/stmtctx` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

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

The package is unchanged from the comparison source; no Go production or test
file required a fix in this batch.

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

## Validation and risk

Profile: **WIP** for this documentation-only boundary audit. No production,
test, or Bazel source changed, so no new regression test or package-complete
Ready claim is made.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/sessionctx/stmtctx -count=1
# PASS ok github.com/pingcap/tidb/pkg/sessionctx/stmtctx 2.811s
```

The wrapper enabled failpoints before the exact Go-master suite and disabled
them during teardown. Rust source, Bazel, and module files were unchanged;
`make bazel_prepare` and Ready lint were not required for this docs-only batch.
Not verified: a dependency-closed Rust package implementation, full Bazel
shard execution, or the broader repository Ready profile. Correctness,
compatibility, and performance risk are unchanged because this batch modifies
documentation only.

This receipt certifies the bounded `pkg/sessionctx/stmtctx` inventory and
ownership boundary; it is not a repository-wide parity claim.
