// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! testport batch b046 — `pkg/statistics` part5 (items 201–260 of the
//! deterministic (file path, line) ordering of every `func Test*` under
//! `pkg/statistics/**` on `origin/master`).
//!
//! Every Go test in this slice lives under
//! `pkg/statistics/handle/handletest/{analyze,initstats,lockstats}` and
//! `handle_test.go`, and each one drives a full TiDB session through
//! `testkit.CreateMockStoreAndDomain` / `session.CreateStoreAndBootstrap`
//! (`ANALYZE`, `LOCK STATS`, `SHOW STATS_LOCKED`, stats-handle storage
//! updates). The tidb-stats crate owns only leaf primitives; the session,
//! domain, and statistics-handle runtime are external boundaries, so these
//! ports are recorded as go-parity-gap ignores rather than approximations.
//! The pure leaf that does exist (`GetConcurrency` from
//! `handle/initstats/load_stats.go`) is already pinned by
//! `init_stats_concurrency_source.rs`.

// -------------------------------------------------------------------------
// go-parity-gap markers. Each `#[ignore]`d test names one Go test from
// origin/master whose behavior has no Rust surface yet.
// -------------------------------------------------------------------------

// handletest/analyze/analyze_test.go::TestAnalyzeWithDynamicPartitionPruneMode
#[test]
#[ignore = "go-parity-gap: drives ANALYZE through a testkit session with dynamic partition prune mode; no session/domain harness exists in tidb-stats"]
fn analyze_with_dynamic_partition_prune_mode() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/analyze/analyze_test.go::TestFMSWithAnalyzePartition
#[test]
#[ignore = "go-parity-gap: partition ANALYZE FM-sketch maintenance via session + StatsHandle storage; unported boundary"]
fn fms_with_analyze_partition() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/analyze/analyze_test.go::TestAnalyzeMetricsCounters
#[test]
#[ignore = "go-parity-gap: asserts prometheus analyze counters emitted by the session-driven analyze executor; metrics runtime not ported"]
fn analyze_metrics_counters() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestEmptyTable
#[test]
#[ignore = "go-parity-gap: requires testkit mock store/domain and an analyzed table via SQL; session harness not ported"]
fn empty_table() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestColumnIDs
#[test]
#[ignore = "go-parity-gap: needs StatsHandle.GetPhysicalTableStats plus cardinality.GetRowCountByColumnRanges over a session-built HistColl after DROP COLUMN; handle runtime not ported"]
fn column_ids() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestDurationToTS
#[test]
#[ignore = "go-parity-gap: handle/util.DurationToTS is not ported to tidb-stats (oracle.ComposeTS of millisecond duration)"]
fn duration_to_ts() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestVersion
#[test]
#[ignore = "go-parity-gap: builds handle.NewHandle against a mock store, rewrites mysql.stats_meta versions via SQL, and checks MaxTableStatsVersion update semantics; storage-backed handle not ported"]
fn version() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestLoadHist
#[test]
#[ignore = "go-parity-gap: LoadHist drives the session-analyzed histogram reload path through StatsHandle; unported"]
fn load_hist() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestCorrelation
#[test]
#[ignore = "go-parity-gap: verifies column correlation after session ANALYZE with index usage; needs handle + planner cardinality surface"]
fn correlation() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestMergeGlobalTopN
#[test]
#[ignore = "go-parity-gap: merges partition-level TopN into global stats through session ANALYZE on a partitioned table; combined merge entrypoint not ported to tidb-stats"]
fn merge_global_topn() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestStaticPartitionPruneMode
#[test]
#[ignore = "go-parity-gap: session-driven ANALYZE under static partition prune mode; no session harness"]
fn static_partition_prune_mode() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestMergeIdxHist
#[test]
#[ignore = "go-parity-gap: merges partition index histograms via session ANALYZE + StatsHandle; unported"]
fn merge_idx_hist() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestPartitionPruneModeSessionVariable
#[test]
#[ignore = "go-parity-gap: toggles the tidb_partition_prune_mode session variable around ANALYZE; session variables not ported"]
fn partition_prune_mode_session_variable() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestDuplicateFMSketch
#[test]
#[ignore = "go-parity-gap: exercises handle FMSketch dedup across session ANALYZE on partitions; FM-sketch handle path unported"]
fn duplicate_fm_sketch() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestIndexFMSketch
#[test]
#[ignore = "go-parity-gap: index-level FMSketch collection through session ANALYZE and handle storage; unported"]
fn index_fm_sketch() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestLoadHistogramWithCollate
#[test]
#[ignore = "go-parity-gap: loads collation-sensitive histograms through the handle from storage rows written by session ANALYZE; collate-aware load path unported"]
fn load_histogram_with_collate() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestStatsCacheUpdateSkip
#[test]
#[ignore = "go-parity-gap: uses testfailpoint to pause the handle Update loop and asserts cache skip behavior; failpoint + cache runtime unported"]
fn stats_cache_update_skip() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestIncrementalModifyCountUpdate
#[test]
#[ignore = "go-parity-gap: incremental modify-count updates via DML + handle Update against a mock store; unported"]
fn incremental_modify_count_update() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestFlushPendingStatsDeltaBeforeAnalyze
#[test]
#[ignore = "go-parity-gap: pending stats-delta flush ordering before session ANALYZE; delta flush runtime is an external boundary (ddl_stats_delta leaf only)"]
fn flush_pending_stats_delta_before_analyze() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestRecordHistoricalStatsToStorage
#[test]
#[ignore = "go-parity-gap: writes historical stats snapshots to mysql.stats_history via the handle; historical-stats storage path unported (historical_stats.rs holds labels only)"]
fn record_historical_stats_to_storage() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestEvictedColumnLoadedStatus
#[test]
#[ignore = "go-parity-gap: eviction of column loaded status inside the session-bound stats cache; LFU/cache eviction runtime unported"]
fn evicted_column_loaded_status() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestUninitializedStatsStatus
#[test]
#[ignore = "go-parity-gap: inspects IsStatsInitized/Status of columns loaded by the handle after session ANALYZE; handle load path unported"]
fn uninitialized_stats_status() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestIssue39336
#[test]
#[ignore = "go-parity-gap: issue-39336 regression requiring session DDL + ANALYZE + handle reload; unported"]
fn issue39336() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestInitStatsLite
#[test]
#[ignore = "go-parity-gap: lite initstats bootstraps stats from storage at domain start; initstats loader runtime unported (only GetConcurrency policy exists as a leaf)"]
fn init_stats_lite() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestInitStatsLiteRecordsSynthesizedColumnStats
#[test]
#[ignore = "go-parity-gap: asserts lite initstats synthesizes pseudo-ish column stats records during bootstrap; loader runtime unported"]
fn init_stats_lite_records_synthesized_column_stats() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestSkipMissingPartitionStats
#[test]
#[ignore = "go-parity-gap: global-stats load skipping missing partition stats via handle storage reads; unported"]
fn skip_missing_partition_stats() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestStatsCacheUpdateTimeout
#[test]
#[ignore = "go-parity-gap: injects lock timeouts into the handle Update transaction via failpoints; failpoint + storage runtime unported"]
fn stats_cache_update_timeout() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestLoadStatsForBitColumn
#[test]
#[ignore = "go-parity-gap: BIT-column histogram decode through the handle load path; session + storage required"]
fn load_stats_for_bit_column() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestStatsCacheShouldNotCacheSystemTable
#[test]
#[ignore = "go-parity-gap: asserts system tables are excluded from the live stats cache; cache admission runs behind the session/domain handle"]
fn stats_cache_should_not_cache_system_table() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestStatsCacheShouldNotCacheTemporaryTable
#[test]
#[ignore = "go-parity-gap: asserts temporary tables are excluded from the live stats cache; cache admission runs behind the session/domain handle"]
fn stats_cache_should_not_cache_temporary_table() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestPrunedIndexesNoAsyncStatsLoad
#[test]
#[ignore = "go-parity-gap: async stats-load skipping of pruned indexes via session queries + async load worker; unported"]
fn pruned_indexes_no_async_stats_load() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestPrunedIndexesNoAsyncStatsLoadPartitioned
#[test]
#[ignore = "go-parity-gap: same async-load skip on partitioned tables via session + handle; unported"]
fn pruned_indexes_no_async_stats_load_partitioned() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/handle_test.go::TestPrunedIndexesNoAsyncStatsLoadPartitionedStatic
#[test]
#[ignore = "go-parity-gap: same async-load skip under static partition prune mode; unported"]
fn pruned_indexes_no_async_stats_load_partitioned_static() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/initstats/init_stats_test.go::TestLiteInitStatsWithTableIDs
#[test]
#[ignore = "go-parity-gap: concurrent lite initstats bootstrap scoped to table IDs against a bootstrapped store; loader runtime unported"]
fn lite_init_stats_with_table_ids() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/initstats/init_stats_test.go::TestNonLiteInitStatsWithTableIDs
#[test]
#[ignore = "go-parity-gap: non-lite initstats bootstrap scoped to table IDs; loader runtime unported"]
fn non_lite_init_stats_with_table_ids() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/initstats/init_stats_test.go::TestConcurrentlyInitStatsWithMemoryLimit
#[test]
#[ignore = "go-parity-gap: initstats concurrency under a memory limit; needs store + mem-tracking loader"]
fn concurrently_init_stats_with_memory_limit() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/initstats/init_stats_test.go::TestConcurrentlyInitStatsWithoutMemoryLimit
#[test]
#[ignore = "go-parity-gap: initstats concurrency without memory limit; needs store-backed loader"]
fn concurrently_init_stats_without_memory_limit() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/initstats/init_stats_test.go::TestDropTableBeforeConcurrentlyInitStats
#[test]
#[ignore = "go-parity-gap: dropped-table race during concurrent initstats; session/store race harness unported"]
fn drop_table_before_concurrently_init_stats() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/initstats/init_stats_test.go::TestDropTableBeforeNonLiteInitStats
#[test]
#[ignore = "go-parity-gap: dropped-table race during non-lite initstats; session/store race harness unported"]
fn drop_table_before_non_lite_init_stats() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/initstats/init_stats_test.go::TestSkipStatsInitWithSkipInitStats
#[test]
#[ignore = "go-parity-gap: tidb_skip_init_stats session variable gating of bootstrap; session variables unported"]
fn skip_stats_init_with_skip_init_stats() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/initstats/init_stats_test.go::TestNonLiteInitStatsAndCheckTheLastTableStats
#[test]
#[ignore = "go-parity-gap: asserts the highest-physical-ID table finishes non-lite initstats; loader runtime unported"]
fn non_lite_init_stats_and_check_the_last_table_stats() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/lockstats/lock_partition_stats_test.go::TestLockAndUnlockPartitionStats
#[test]
#[ignore = "go-parity-gap: LOCK STATS t PARTITION p0 lifecycle via SQL + mysql.stats_table_locked; lock mutation runtime is an external boundary (only the query filter and diagnostics leaves exist)"]
fn lock_and_unlock_partition_stats() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/lockstats/lock_partition_stats_test.go::TestLockAndUnlockPartitionsStats
#[test]
#[ignore = "go-parity-gap: multi-partition LOCK/UNLOCK STATS via SQL; lock mutation runtime unported"]
fn lock_and_unlock_partitions_stats() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/lockstats/lock_partition_stats_test.go::TestLockAndUnlockPartitionStatsRepeatedly
#[test]
#[ignore = "go-parity-gap: repeated lock/unlock idempotence through SQL; lock mutation runtime unported"]
fn lock_and_unlock_partition_stats_repeatedly() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/lockstats/lock_partition_stats_test.go::TestSkipLockPartition
#[test]
#[ignore = "go-parity-gap: already-locked partition skip warnings; needs lock SQL state + SHOW WARNINGS session surface (sorted-skip message leaf is pinned by lock_messages_source.rs)"]
fn skip_lock_partition() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/lockstats/lock_partition_stats_test.go::TestUnlockOnePartitionOfLockedTableWouldFail
#[test]
#[ignore = "go-parity-gap: unlocking a single partition of a fully locked table must fail; unlock validation runtime unported"]
fn unlock_one_partition_of_locked_table_would_fail() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/lockstats/lock_partition_stats_test.go::TestUnlockTheUnlockedTableWouldGenerateWarning
#[test]
#[ignore = "go-parity-gap: unlocking a never-locked table emits a warning; unlock runtime + warning surface unported"]
fn unlock_the_unlocked_table_would_generate_warning() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/lockstats/lock_partition_stats_test.go::TestSkipLockALotOfPartitions
#[test]
#[ignore = "go-parity-gap: many-partition lock skip batching (>512 partitions) through SQL; lock runtime unported"]
fn skip_lock_a_lot_of_partitions() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/lockstats/lock_partition_stats_test.go::TestReorganizePartitionShouldCleanUpLockInfo
#[test]
#[ignore = "go-parity-gap: REORGANIZE PARTITION cleanup of mysql.stats_table_locked via DDL + session; DDL lock-info cleanup unported"]
fn reorganize_partition_should_clean_up_lock_info() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/lockstats/lock_partition_stats_test.go::TestDropPartitionShouldCleanUpLockInfo
#[test]
#[ignore = "go-parity-gap: DROP PARTITION cleanup of lock info via DDL + session; unported"]
fn drop_partition_should_clean_up_lock_info() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/lockstats/lock_partition_stats_test.go::TestTruncatePartitionShouldCleanUpLockInfo
#[test]
#[ignore = "go-parity-gap: TRUNCATE PARTITION cleanup of lock info via DDL + session; unported"]
fn truncate_partition_should_clean_up_lock_info() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/lockstats/lock_partition_stats_test.go::TestExchangePartitionShouldChangeNothing
#[test]
#[ignore = "go-parity-gap: EXCHANGE PARTITION must leave lock info untouched; DDL + lock runtime unported"]
fn exchange_partition_should_change_nothing() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/lockstats/lock_partition_stats_test.go::TestNewPartitionShouldBeLockedIfWholeTableLocked
#[test]
#[ignore = "go-parity-gap: newly added partition inherits whole-table lock coverage; lock runtime unported"]
fn new_partition_should_be_locked_if_whole_table_locked() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/lockstats/lock_partition_stats_test.go::TestUnlockSomePartitionsWouldUpdateGlobalCountCorrectly
#[test]
#[ignore = "go-parity-gap: partial unlock keeps the global locked count consistent; unlock runtime unported"]
fn unlock_some_partitions_would_update_global_count_correctly() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/lockstats/lock_table_stats_test.go::TestLockAndUnlockTableStats
#[test]
#[ignore = "go-parity-gap: whole-table LOCK/UNLOCK STATS lifecycle via SQL; lock mutation runtime unported"]
fn lock_and_unlock_table_stats() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/lockstats/lock_table_stats_test.go::TestLockAndUnlockPartitionedTableStats
#[test]
#[ignore = "go-parity-gap: locking a partitioned table locks all partitions; lock runtime unported"]
fn lock_and_unlock_partitioned_table_stats() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/lockstats/lock_table_stats_test.go::TestLockTableAndUnlockTableStatsRepeatedly
#[test]
#[ignore = "go-parity-gap: repeated table lock/unlock idempotence via SQL; lock runtime unported"]
fn lock_table_and_unlock_table_stats_repeatedly() {
    unreachable!("gated by go-parity-gap ignore")
}

// handletest/lockstats/lock_table_stats_test.go::TestLockAndUnlockTablesStats
#[test]
#[ignore = "go-parity-gap: multi-table LOCK/UNLOCK STATS in one statement via SQL; lock runtime unported"]
fn lock_and_unlock_tables_stats() {
    unreachable!("gated by go-parity-gap ignore")
}
