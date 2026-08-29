# `pkg/statistics/handle/autoanalyze/exec` package audit

Reference: TiDB Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 46 | `54e30b2c58b8e14798fd55374f97ddb9cce81bca` |
| `exec.go` | 157 | `8d7675dd3849f00f486aad176605f43a7b85ef37` |
| `exec_test.go` | 184 | `f3fc031207c82abd270f377efdf00d7f87d11031` |

All 387 lines were read. The package has three tests and no benchmark:
`TestExecAutoAnalyzes`, `TestExecAutoAnalyzeRewritesLegacyStatsVersionToV2`,
and `TestKillInWindows`.

## Go behavior

`AutoAnalyze` and `RunAnalyzeStmt` execute through the current session with
stats-version-2, analyze-snapshot, partition-prune, and process-tracking
options. They allocate and always release an auto-analyze process ID, recover
and log panics, record success/failure metrics, emit the escaped legacy-version
rewrite warning, and surface query interruption. The package also reads the
three global parameters and contains private ratio/time parsing helpers.

The tests validate the integrated execution and cache result, exact escaped
warning and version rewrite for table/partition/global statistics, and killing
an analyze when it leaves the configured window. They do not test the private
parsers in isolation.

## Rust comparison and decision

Rust had an unconsumed public `parse_auto_analyze_ratio` leaf and a compatibility
module that re-exported the priority-queue runtime's minute-based window type.
Six source-absent tests exercised those public helpers. Repository-wide symbol
tracing found no production caller. Rust had no `AutoAnalyze`/`RunAnalyzeStmt`
session execution owner and none of the three Go tests was represented.

The two modules, their six tests, and the duplicate root aliases were removed.
The minute-based window type that was inside `auto_analyze_runtime` was
subsequently removed by the whole `priorityqueue` package audit; it was not
this package's `ParseAutoAnalysisWindow` result.

Completing `autoanalyze/exec` is not dependency-closed while the ordinary
statistics handle/types and current-session ANALYZE execution path remain
unclaimed. The package must land later with all three artifacts and all three
integrated tests, not as parser leaves.

## WIP validation

- `cargo check --locked -p tidb-stats` passed.
- `cargo nextest run --locked -p tidb-stats -E 'not test(/bench/)' --no-fail-fast`
  passed: 375 run, 375 passed, 154 skipped.
- `rustfmt --edition 2021 --check crates/tidb-stats/src/lib.rs` passed.
- `git diff --check` passed.

No Go or Bazel source changed, so `make bazel_prepare` was not required. This
is a WIP package audit, not a repository-wide Ready parity claim.
