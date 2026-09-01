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

## Rust implementation and integration

`tidb-stats-handle-autoanalyze-exec` now owns the package as a whole. Its
`auto_analyze` and `run_analyze_stmt` functions apply the v2,
analyze-snapshot, partition-prune, current-session, and process-tracking
options to the shared restricted executor. Process-ID release and process
untracking use drop guards, including the recovered-panic path. The package
also owns Go's success/failure metrics and logging, escaped legacy-version
warning, global parameter reads, ratio parser, and fixed-offset analysis-window
parser.

The production priority-queue source calls this shared path directly. It no
longer pre-renders identifiers or invokes a cache-only/server-only ANALYZE
shortcut. A checked-out system session supplies its registered connection ID;
the pool owns that ID for the session lifetime, while the per-statement guard
still invokes the generator's release operation. The global auto-analyze
process list is connected to the live process registry, and the domain's
post-statistics-GC window check interrupts registered analyzes outside the
configured interval, in Go's worker order.

The original three Go tests map to Rust coverage as follows:

| Go test | Rust evidence |
| --- | --- |
| `TestExecAutoAnalyzes` | package option/execution tests plus `auto_analyze_exec_uses_live_tracking_and_current_session_like_go` |
| `TestExecAutoAnalyzeRewritesLegacyStatsVersionToV2` | `source_legacy_rewrite_still_executes_as_version_two` |
| `TestKillInWindows` | `auto_analyze_window_check_kills_only_outside_the_window_like_go` |

There are no omitted build variants, generated inputs, fixtures, support
files, benchmarks, or fuzz tests at the pinned package boundary. The package
is complete; scheduling and job selection remain owned by the parent and
priority-queue packages.

## WIP validation

- `cargo check --offline -p tidb-server -p tidb-stats-handle-autoanalyze-exec`
  passed.
- `cargo test --offline -p tidb-stats-handle-autoanalyze-exec --lib -- --nocapture`
  passed: 5 passed.
- `cargo test --offline -p tidb-server --lib auto_analyze_exec_uses_live_tracking_and_current_session_like_go -- --nocapture`
  passed.
- `cargo test --offline -p tidb-server --lib auto_analyze_window_check_kills_only_outside_the_window_like_go -- --nocapture`
  passed.
- `cargo fmt --all -- --check` and `git diff --check` passed.

The broader existing priority-queue integration test currently fails before
execution, while observing asynchronous removal of a dropped-table queue job
(`current_jobs` is 2 instead of 1). The changed exec path has not run at that
assertion. This is recorded rather than masking the separate queue/lifecycle
failure in this package receipt.

No Go or Bazel source changed, so `make bazel_prepare` was not required. This
is a WIP package audit, not a repository-wide Ready parity claim.
