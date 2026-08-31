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

Pinned `TestStatsCacheShouldNotCacheSystemTable` is covered through the
ordinary Unistore session path. After ANALYZE publishes the source test's one
user-table object, `SHOW STATS_META` and `SHOW STATS_HEALTHY` leave the
canonical loaded-statistics cache receipt unchanged at one. Rust's separate
statement-facing snapshot records pseudo load attempts for system tables, but
`SharedStats` inserts only `TableStatsState::Loaded` objects into its canonical
cache and the planner synthesizes pseudo statistics on a cache miss. Those
attempt states therefore are not cached system-table statistics and removing
them would not match the Go cache contract.

Pinned `TestPrunedIndexesNoAsyncStatsLoad`,
`TestPrunedIndexesNoAsyncStatsLoadPartitioned`, and
`TestPrunedIndexesNoAsyncStatsLoadPartitionedStatic` now have one executable
production-path Rust regression in
`tidb-executor::driver::catalog::statistics_request_tests`. It constructs the
same 13 `a`-prefixed and 14 unrelated indexes, invokes ordinary planning with
zero synchronous wait and threshold one, and inspects the process-wide async
demand map. Non-partitioned and dynamic pruning request a nonempty subset of
at most ten `a` indexes and no unrelated index. Static pruning expands demand
to every physical partition and, matching the pinned Go test's deliberately
weaker contract, requires a nonempty `a` set and no unrelated index without
asserting the ten-index cap. The test reads the production planner rule,
retained-index filter, static partition expansion, and async publication
boundary rather than a disconnected carrier.

Pinned `TestLoadStatsForBitColumn` maps to the production
`tidb-exec::cluster_stats_load::decode_bound` BIT branch and its executable
four-case regression. It preserves Go's decimal storage form and reconstructs
the exact values for `BIT(1)` 0/1, `BIT(2)` 2/3, quoted-byte `BIT(6)` 48/49,
and quoted-byte `BIT(7)` 97/98. The string/collation-key and index-key byte
branches remain separate, as they are in Go.

Pinned `TestIssue39336` now has an exact Unistore production-path regression.
With empty SQL mode, Analyze v2, and dynamic partition pruning, Rust accepts
the source test's nine DATETIME(3) rows (including the zero-in month values),
analyzes both partitions with zero TopN, merges the global column statistics,
and exposes exactly one matching `SHOW ANALYZE STATUS` row in `finished`
state.

Pinned `TestLoadHistogramWithCollate` maps directly to
`cluster_stats_load::decode_bound` and
`a_string_column_bound_stays_raw_bytes_because_it_may_be_a_collation_key`.
The production loader follows Go's `HistogramFromStorageWithPriority` string
branch: a non-ENUM/SET string column is read back as blob bytes because a new
collation's stored weight string may exceed the declared `flen` or be invalid
text. The exact ten-byte `VARCHAR` shape therefore loads without applying a
second character conversion.

Pinned `TestStatsCacheUpdateSkip` maps to the production metadata-version
probe plus `an_unchanged_read_publishes_nothing`. Equal table versions and an
unchanged tracked set skip the full statistics read, retain the exact
published `Arc<StatsSnapshot>`, and do not increment the reload count. This is
stronger executable evidence for the source test's equal cached table before
and after `Handle.Update`.

Pinned `TestStatsCacheUpdateTimeout` maps to the same production reload pass
and `a_failed_pass_keeps_the_previous_snapshot_published`. A failed system-row
read increments the failure counter but publishes nothing, so the prior table
version, realtime count, and modify count stay in force. Rust's background
worker reports that failure through its observable counter/log rather than a
direct test call to Go's exported `Handle.Update`; no cache mutation occurs in
either path.

Pinned `TestUninitializedStatsStatus` now has an exact ordinary-session
regression. CREATE plus delta flush leaves DDL placeholder columns/indexes
uninitialized, `SHOW STATS_HISTOGRAMS` exposes no rows, and EXPLAIN retains
`stats:pseudo` with `tidb_enable_pseudo_for_outdated_stats` both enabled and
disabled. The lower-level planner receipt also proves that reduced histogram
metadata retains the realtime row count while the table remains pseudo.

Pinned `TestSkipMissingPartitionStats` now has an exact Unistore
production-path regression. Dynamic partition ANALYZE over p0 and p1 with p2
missing publishes the logical/global count as six, modify count as two, and
all three columns plus `idx_b` as initialized statistics. This exercises the
ordinary session variables, storage merge, canonical cache, and SHOW surfaces
rather than a standalone merge carrier.

Pinned `TestVersion` exposed that Rust's live periodic and post-ANALYZE/LOAD
refresh seam still bypassed the already ported parent cache update. It scanned
all tracked metadata, compared a whole statement-facing snapshot, and rebuilt
that snapshot on any difference. The live seam now calls the canonical
`StatsCacheImpl.Update` equivalent for every refresh: the source reads the six
ordered `stats_meta` fields, applies the five-lease watermark and optional
target ID predicate, rejects an older row when the cached table has the same
schema timestamp, reuses metadata-only payload when the histogram version
allows it, and publishes ten-row batches with Go's targeted
`SkipMoveForward`. The cache-specific whole-snapshot version probe and refresh
helpers, along with their tests and documentation, were removed. The exact
regression proves that a cached version four cannot be replaced or reloaded by
a later version-one row, retains the same published table object, and keeps the
cache maximum at four. Go source comparison also showed that the post-ANALYZE
target list is task-derived rather than the historical-dump list: Rust now
passes the logical ID plus every physical partition ID even in static mode,
and keeps independent index task IDs in that cache-update receipt. The same
row-boundary audit removed Rust's saturation of unsigned `stats_meta.count`:
pinned Go reads that slot with `chunk.Row.GetInt64`, so values above
`i64::MAX` retain their two's-complement signed result. Partition refresh
targets now also retain Go's separate identities: the storage read uses the
physical partition ID, while schema lookup and `TableStatsFromStorage` receive
the unchanged parent `TableInfo` rather than a clone whose table ID was
rewritten to the partition ID.

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
- `cargo test -p tidb-executor pruned_indexes_do_not_enter_async_statistics_demand -- --nocapture`
  passed: 1 passed.
- `cargo test -p tidb-exec bit_column_bounds_load_from_the_four_go_decimal_storage_cases -- --nocapture`
  passed: 1 passed.
- `cargo test -p tidb-server partition_global_analyze_finishes_with_zero_in_datetime_values -- --nocapture`
  passed outside the sandbox after the in-sandbox run stopped at the known
  macOS `sysctl hw.memsize` restriction.
- `cargo test -p tidb-server show_stats_does_not_cache_system_table_statistics -- --nocapture`
  passed outside the sandbox; the canonical loaded cache remained unchanged
  across both SHOW scans.
- `cargo test -p tidb-exec a_string_column_bound_stays_raw_bytes_because_it_may_be_a_collation_key -- --nocapture`
  passed: 1 passed.
- `cargo test -p tidb-exec an_unchanged_read_publishes_nothing -- --nocapture`
  passed: 1 passed.
- `cargo test -p tidb-exec a_failed_pass_keeps_the_previous_snapshot_published -- --nocapture`
  passed: 1 passed.
- `cargo test -p tidb-server uninitialized_statistics_remain_hidden_and_pseudo -- --nocapture`
  passed outside the sandbox: 1 passed.
- `cargo test -p tidb-server global_statistics_skip_missing_partition_like_go -- --nocapture`
  passed outside the sandbox: 1 passed.
- `cargo test -p tidb-exec an_older_stats_version_cannot_move_the_shared_cache_backward -- --nocapture`
  passed: 1 passed.
- `cargo test -p tidb-exec stats_watch::tests -- --nocapture` passed: 20 passed.
- `cargo test -p tidb-stats-handle-cache` passed: 9 passed.
- `cargo test -p tidb-exec post_analyze_cache_ids_come_from_tasks_not_history_targets -- --nocapture`
  passed: 1 passed.
- `cargo test -p tidb-server build_global_level_stats_matches_go -- --nocapture`
  passed outside the sandbox: 1 passed.
- `cargo test -p tidb-exec stats_meta_unsigned_count_uses_go_get_int64_bits -- --nocapture`
  passed: 1 passed.
- `cargo test -p tidb-exec partition_stats_target_keeps_parent_table_info_like_go -- --nocapture`
  passed: 1 passed.
- `cargo check -p tidb-exec -p tidb-server` passed.
- `cargo test -p tidb-exec cache_update_preserves_and_refreshes_resident_histogram_payload -- --nocapture`
  passed: 1 passed.
- `cargo test -p tidb-exec initial_stats_matches_go_table_scope_and_payload_shapes -- --nocapture`
  passed: 1 passed.
- `cargo test -p tidb-exec initial_stats_handles_missing_histograms_and_topn_without_buckets -- --nocapture`
  passed: 1 passed.
- `cargo test -p tidb-exec cluster_stats_load::tests -- --nocapture` passed:
  10 passed.
- `cargo test -p tidb-exec real_tikv_stats::tests -- --nocapture` passed:
  1 passed.
- `cargo fmt --all -- --check` passed.
- `make lint` passed for this batch's Ready gate.

The prior gate had 271 passing and 105 skipped tests; removing one duplicate
utility assertion and 72 ignored empty functions accounts for the exact new
totals. No Go or Bazel source changed, so `make bazel_prepare` was not
required. This is a WIP package audit, not a repository-wide Ready claim.
