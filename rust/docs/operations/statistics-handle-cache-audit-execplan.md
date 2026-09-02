# `pkg/statistics/handle/cache` Go-master audit

This living ExecPlan records the atomic root-package comparison required by
`AGENTS.md`. The source snapshot is Go `master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec`.

## Scope

Read all five root artifacts (`BUILD.bazel`, `statscache.go`,
`statscacheinner.go`, `statscache_test.go`, and `bench_test.go`) before editing.
The package has 1,051 lines, two tests, six benchmark shapes, and no fixtures,
generated/platform variants, or other support artifacts.

## Progress

- [x] Reconciled the root cache implementation and tests with the c605 source.
- [x] Removed the Rust-only process-wide `StatsTableRowCache` from
      `tidb-stats-handle-cache`.
- [x] Implemented statement-local `TableSizeStats` in the cluster statistics
      loader, including fixed/variable width and partition/global-index
      accounting, negative-size clamping, and fresh row/length maps.
- [x] Passed Go's `needColLength` decision through the planner/session boundary
      so TABLE_ROWS-only scans skip `stats_histograms`.
- [x] Applied estimates to a statement scratch catalog only; failed reads
      clear size values for that statement, matching nil Go getters.
- [x] Added focused regressions for statement locality, negative-size
      clamping, histogram-read skipping, and zero-on-read-error.
- [x] Refreshed the package receipt and parent/top-level parity plans.
- [ ] Continue the rolling whole-repository audit with the next complete Go
      package; this plan does not claim repository-wide parity.

## Validation

WIP tests passed for the root owner, cluster storage reads, statement-local
size accounting, and the server information-schema integration. Ready
validation before the package commit must include the pinned failpoint-wrapped
Go package suite, Rust owner/consumer tests and checks, pinned Rust formatting,
`make lint`, and `git diff --check`.

## Risks and decisions

- The Go source deleted `stats_table_row_cache.go`; retaining its Rust global
  cache would expose stale cross-statement values and violate package parity.
- Rust's catalog overlay remains an internal transport for materialized virtual
  table rows, but it is built per statement and never mutates the shared
  catalog on success or failure.
- No Go or Bazel source changed, so `make bazel_prepare` is not required for
  this Rust-only package batch.
