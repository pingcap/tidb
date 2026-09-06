# `pkg/statistics/handle/cache` Go-master audit

This living ExecPlan records the atomic root-package comparison required by
`AGENTS.md`. The source snapshot was Go `master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec`; the 2026-09-06 round re-pinned
the comparison against the repository's current Go tree tip.

## Scope

Read all root artifacts before editing: `BUILD.bazel`, `statscache.go`,
`statscacheinner.go`, `stats_table_row_cache.go`, `statscache_test.go`, and
`bench_test.go`. At the current tip the package has six artifacts and 1,263
lines, two tests, six benchmark shapes, and no fixtures, generated/platform
variants, or other support artifacts.

## Progress

- [x] Reconciled the root cache implementation and tests with the c605 source.
- [x] Removed the Rust-only process-wide `StatsTableRowCache` from
      `tidb-stats-handle-cache` (correct against c605, where upstream #69955
      had deleted `stats_table_row_cache.go`).
- [x] Implemented statement-local `TableSizeStats` in the cluster statistics
      loader, including fixed/variable width and partition/global-index
      accounting, negative-size clamping, and fresh row/length maps.
- [x] Passed Go's `needColLength` decision through the planner/session boundary
      so TABLE_ROWS-only scans skip `stats_histograms`.
- [x] Re-pinned against the current Go tree tip (2026-09-06): #69955 is NOT in
      this line, `stats_table_row_cache.go` is present, and the
      information-schema reader consumes the process-wide
      `cache.TableRowStatsCache`. Restored the port as
      `tidb-stats-handle-cache::stats_table_row_cache` with Go's exact refresh
      contract: both restricted reads succeed or nothing is copied, upsert-only
      `maps.Copy` semantics, zero defaults for absent IDs.
- [x] Rewired `ClusterTableStorageStatsProvider` to Go's reader flow:
      refresh by every visible table/partition ID, warn-only on a failed
      restricted read, then serve the returned values from the cache —
      fixing the client-visible divergence where a failed refresh zeroed the
      statement instead of retaining prior values.
- [x] Added focused regressions: cache-contract unit tests and the
      store-backed `the_table_row_size_cache_serves_previous_values_after_a_failed_refresh`
      (sensitivity proven by breaking the both-or-nothing copy).
- [x] Refreshed the package receipt and this plan.
- [ ] Continue the rolling whole-repository audit with the next complete Go
      package; this plan does not claim repository-wide parity.

## Validation

Ready validation for the 2026-09-06 round: owner crate suite green (11/11),
store-backed regression green, full `tidb-exec` target green except eight
failures that reproduce on the pristine tip (sibling in-flight work, verified
by stash), clippy clean in every file this batch touches, fmt clean in every
file this batch touches (`--all --check` still reports four pre-existing
sibling files outside this batch's scope). No Go or Bazel source changed, so
`make bazel_prepare` is not required.

## Risks and decisions

- Upstream #69955 (which deleted `stats_table_row_cache.go`) is not an
  ancestor of this branch's Go tree; parity targets the tree that IS pinned,
  not upstream HEAD. If the fork later absorbs #69955, the provider-owned
  cache collapses back to the statement-local path by deleting the refresh
  call — the `TableSizeStats` estimators are already shared.
- Go's cache is process-global; the Rust provider owns one per instance
  (today per session). Because every statement refreshes all visible IDs
  before reading, cross-instance staleness is observable only inside the
  failure window, which now retains values exactly like Go.
- The histogram-read skip for TABLE_ROWS-only statements is kept as a
  documented narrow: this Go line's reader-side `UpdateByID` still reads
  lengths, but the skipped values only feed columns the plan did not retain,
  so client-visible output is identical.
- Rust's catalog overlay remains an internal transport for materialized virtual
  table rows, but it is built per statement and never mutates the shared
  catalog on success or failure.
- No Go or Bazel source changed, so `make bazel_prepare` is not required for
  this Rust-only package batch.
