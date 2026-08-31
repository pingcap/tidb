# `pkg/statistics/handle/handletest` package audit

Reference: TiDB Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 36 | `4a8f726492849fd6707d631610baaa85ddc33a1f` |
| `handle_test.go` | 1,417 | `b04b4f7b9c47044f350eddd0c4f57d1500edd057` |
| `main_test.go` | 34 | `6bf105a34ef3c2be47a6165976067f01ce47f80a` |

All 1,487 lines were read. The package has 30 tests and no benchmark.

## Go behavior

This is an external test package, not a production implementation package.
Its tests drive the ordinary statistics handle through mock stores, domains,
sessions, DDL/DML, ANALYZE, cache reload and eviction, failpoints, historical
storage, partition/global statistics, FM sketches, async loading, collation
and BIT decoding, planner cardinality, and system/temporary-table filtering.
`TestEvictedColumnLoadedStatus` is explicitly skipped by the Go source.

## Rust comparison and decision

Rust had one origin/master batch carrier containing 29 ignored empty functions
and a duplicate `DurationToTS` assertion. The other parent tests were absent.
An ignored empty function executes none of the Go behavior. `DurationToTS` is
owned and tested by the complete `pkg/statistics/handle/util` transcreation in
`tidb-stats-handle-util`, so duplicating it here did not transcreate this test
package.

The parent entries and the mixed carrier were removed. The package remains
unclaimed until the ordinary handle/session/domain/storage integration surface
can support all three artifacts and 30 tests atomically.

## Implemented root-package gaps

Pinned `TestIncrementalModifyCountUpdate` exposed a production mismatch rather
than a missing test carrier. Go samples at `AnalyzeResults.Snapshot`, records
`BaseCount` and `BaseModifyCnt`, and saves through a later statistics-handle
transaction. Rust previously sampled and wrote in one transaction and always
stored `modify_count = 0`.

The wired cluster path now uses separate sampling and save transactions. The
save reads the current `stats_meta` row, keeps modifications committed after
sampling, applies both branches of `tidb_enable_analyze_snapshot`, and treats a
newer stored snapshot as Go's successful no-op. Executable storage regressions
cover both count branches and stale-result suppression.

Pinned `TestStatsCacheShouldNotCacheTemporaryTable` is now covered through the
ordinary production routes. LOCAL create and row storage stay in the session
overlay without committing the user's transaction; GLOBAL metadata goes
through the shared DDL path while its rows stay connection-local and are
deleted at commit. Ordinary reads publish no cache object. Explicit ANALYZE
samples the session storage for both kinds and publishes a canonical
`statistics.Table` into the process cache. An analyzed empty GLOBAL table is a
real cache object but `GetStatsTable` still derives a query-time pseudo table
from its zero realtime count. A later shared-catalog rebuild reinstalls the
cached LOCAL planner view rather than losing it with the session-owned
metadata.

The package is still not claimed: this receipt has not yet reconciled all 30
original tests one by one against executable Rust owners and the complete
package validation gate. The temporary-table blocker is closed evidence, not
a substitute for that atomic inventory.

## WIP validation

- `cargo check --locked -p tidb-stats` passed.
- `cargo nextest run --locked -p tidb-stats -E 'not test(/bench/)' --no-fail-fast`
  passed: 270 run, 270 passed, 33 skipped.
- `git diff --check` passed.
- `cargo test -p tidb-session tests_temporary_tables` passed: 24 passed.
- `cargo test -p tidb-server analyze_temporary_table_uses_the_session_storage_overlay`
  passed outside the sandbox (the statistics cache probes macOS memory size).
- `cargo test -p tidb-server global_temporary_analyze_uses_session_rows_and_statistics`
  passed outside the sandbox.

The prior gate had 271 passing and 105 skipped tests; removing one duplicate
utility assertion and 72 ignored empty functions accounts for the exact new
totals. No Go or Bazel source changed, so `make bazel_prepare` was not
required. This is a WIP package audit, not a repository-wide Ready claim.
