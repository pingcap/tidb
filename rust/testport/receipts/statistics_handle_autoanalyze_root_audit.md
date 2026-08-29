# Root `pkg/statistics/handle/autoanalyze` package audit

Reference: TiDB Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 71 | `bb541aeb13beb1caaddcb01c3e6924ba48f09123` |
| `autoanalyze.go` | 920 | `a590ca2f59b563b7d23c7508111f4219ff6c0490` |
| `autoanalyze_test.go` | 615 | `f082ca3fa12b372e94d5de27d0405c710106e724` |

All 1,606 lines were read. The package has 14 tests and no benchmark.

## Go behavior

`statsAnalyze` composes the ordinary statistics handle, process tracker,
priority queue, refresher, and DDL notifier. It owns analyze-job insertion,
start/progress/finish/delete SQL and timezone conversion; cleans stale jobs on
the current or dead instances; chooses the priority-queue or legacy randomized
scheduler; enforces time windows and locks; handles static/dynamic partitions,
new indexes, columnar/special indexes, requested stats versions, batch sizes,
and cache updates; and closes the live worker/queue.

The tests cover the permanent priority-queue setting, locked tables,
predicate/skip-column analyze options, enable/disable semantics, analyzed
state after cache reload, the `NeedAnalyzeTable` decision, empty/out-of-window
tables and indexes, current/dead-instance cleanup SQL, loop time expiry, and
vector-index exclusion. They run through mock stores/domains, sessions,
InfoSchema, statistics cache, DDL events, restricted SQL mocks, failpoints,
server info, and TiFlash fixtures.

## Rust comparison and decision

Rust exposed only `need_analyze_table`, accepting five already-materialized
scalars and returning the private trigger decision. Six source-absent tests
exercised this extracted helper. Repository-wide tracing found no production
caller. It did not own a statistics table, handle, queue, refresher, session,
SQL lifecycle, lock lookup, partition/index scheduling, time window, server
cleanup, or any of the package's observable integration behavior.

The module, root export, and six tests were removed. The independent Rust
statistics table model remains owned by `pkg/statistics`; this audit does not
remove it. The root auto-analyze package remains unclaimed until its handle,
`exec`, `priorityqueue`, `refresher`, lock, notifier, InfoSchema, and session
dependencies can land with all three artifacts and 14 tests atomically.

## WIP validation

- `cargo check --locked -p tidb-stats` passed.
- `cargo nextest run --locked -p tidb-stats -E 'not test(/bench/)' --no-fail-fast`
  passed: 271 run, 271 passed, 105 skipped.
- `rustfmt --edition 2021 --check crates/tidb-stats/src/lib.rs` passed.
- `git diff --check` passed.

No Go or Bazel source changed, so `make bazel_prepare` was not required. This
is a WIP package audit, not a repository-wide Ready parity claim.
