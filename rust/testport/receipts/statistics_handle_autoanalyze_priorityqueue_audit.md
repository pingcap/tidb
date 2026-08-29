# `pkg/statistics/handle/autoanalyze/priorityqueue` package audit

Reference: TiDB Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

The parent package has 22 artifacts and 8,132 lines. Every artifact was read
before the Rust decision. The two nested packages, `calculatoranalysis` and
`intervaltimezone`, remain separate package units for subsequent audits.

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 88 | `6abdf25c503e50c1aac91544e66158e66a2c2fc0` |
| `analysis_job_factory.go` | 402 | `2d7b16bb6f9a556962c4593ab86b7805716bfd5a` |
| `analysis_job_factory_test.go` | 527 | `f8f121a0e92f2a31c8c58b5335337d63731939d5` |
| `calculator.go` | 73 | `e79c46b6931e4c06c223e0fcc6b13cdf01865109` |
| `calculator_test.go` | 148 | `35f65b5b139a4c2a5d3172fa4a93a0d6c88493b6` |
| `dynamic_partitioned_table_analysis_job.go` | 373 | `fa49137d237c3cb9470b73eb025e4c9aedaabef1` |
| `dynamic_partitioned_table_analysis_job_test.go` | 228 | `f6da9fb202bb20f72c2bef26cc1b4dea067bf105` |
| `heap.go` | 202 | `0055af44927a10c0ff37c92a5fe6577c63fe1dbe` |
| `heap_test.go` | 320 | `13590237bd6a346ccf5403ae6ce83444875dee61` |
| `interval.go` | 161 | `0497091358cd2367c259c65be3036204aacb9f66` |
| `interval_test.go` | 419 | `67117724f605d0ba653eba1842dd599bfc1788f7` |
| `job.go` | 200 | `bf01a000d88a71953097dc60d90ad1cc55c4f40c` |
| `job_test.go` | 175 | `9d8dbf292caf60b3dd40fea438fcd17487205878` |
| `main_test.go` | 34 | `91c7de7a6817a5a39d76babc6a90e3cb7592311f` |
| `non_partitioned_table_analysis_job.go` | 278 | `5bd07bf8f69a67b2474f01c927510fba23247469` |
| `non_partitioned_table_analysis_job_test.go` | 207 | `621b3614ac00e6d4a3ce8c472cec50784ddb6f63` |
| `queue.go` | 1,274 | `540af225d5fd2c925b68b0058499150bac196af1` |
| `queue_ddl_handler.go` | 479 | `a7a64a1d822a0bab23f3c2e80c1b1ee1855ccbf9` |
| `queue_ddl_handler_test.go` | 1,213 | `60a761a734a50661365c70f229dc2f7c0b653c96` |
| `queue_test.go` | 812 | `706ff3308b2d985f48261fd9e7788deca46c65e2` |
| `static_partitioned_table_analysis_job.go` | 314 | `a9e9e0244e33d7a75ca27ebef8e25a8b3e877a97` |
| `static_partitioned_table_analysis_job_test.go` | 205 | `f179a922a9816ec3b1b6bb0d6002fe941930cd9b` |

The 11 test artifacts contain 78 assertion tests plus the shared leak-checking
`TestMain`, and no benchmark.

## Go behavior

Go owns one synchronized, live analysis scheduler. It scans real InfoSchema
tables through the statistics handle, constructs concrete table/partition
jobs, ranks them in a keyed max heap, tracks running and retry identities,
processes versioned DML changes and table locks, refreshes durations, and runs
background maintenance tickers. Its DDL notifier mutates that same queue for
all supported schema events. Concrete jobs validate against current metadata
and `mysql.analyze_jobs`, execute through `autoanalyze/exec`, refresh the stats
cache, publish hooks and warnings, and preserve Go's SQL, failure, string, and
JSON contracts.

The tests exercise this integrated behavior through the mock store, domain,
session, statistics handle, DDL notifier, failpoints, concurrency, lifecycle,
and post-ANALYZE cache/storage effects. The heap arithmetic and SQL templates
are private pieces of that package, not independent public APIs.

## Rust comparison and decision

Rust exposed an alternate runtime assembled from caller-implemented
`SessionPort`, `InfoSchemaPort`, `StatisticsPort`, `SqlPort`, clock, hook, and
queue traits. Its queue accepted already-materialized jobs, locks, and DML
versions; it did not scan tables, own sessions, run background tickers, receive
the real notifier, recreate jobs from the current stats cache, or execute via
the ordinary auto-analyze path. Additional public modules exposed heap,
priority, interval, SQL-template, gate, and job-metadata slices. Repository-wide
symbol tracing found no production consumer outside this crate.

This was behaviorally different as well as incomplete: it invented a scalar
`PriorityHeapItem`, exposed source-private helpers, delegated recovery to a
caller closure, used an adapter that reconstructed queue state from snapshots,
and carried acknowledged SQL/error-string and integration gaps. Adding more
ports would preserve the wrong owner boundary rather than implement Go.

Therefore 20 production carrier files (2,412 lines), their compatibility
exports, and 19 test files (2,627 lines) were removed. The tests comprised 92
runnable slice tests and 49 ignored gap tests. The old `b043` receipt was also
removed because it used `origin/master`, mixed three Go packages, and reported
partial functions as ports despite the atomic package rule.

The package remains explicitly unclaimed. It can land only after the ordinary
statistics handle/types/storage, session ANALYZE execution, InfoSchema, lock,
and notifier owners are dependency-complete, together with all 22 artifacts
and the full integrated test surface. Unrelated `statistics/table.go`, root
`autoanalyze`, and `refresher` Rust carriers are not claimed or removed by this
package audit.

## WIP validation

- `cargo check --locked -p tidb-stats` passed.
- `cargo nextest run --locked -p tidb-stats -E 'not test(/bench/)' --no-fail-fast`
  passed: 283 run, 283 passed, 105 skipped.
- `rustfmt --edition 2021 --check crates/tidb-stats/src/lib.rs` passed.
- `git diff --check` passed.

No Go or Bazel source changed, so `make bazel_prepare` was not required. This
is a WIP package audit, not a repository-wide Ready parity claim.
