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

//! Go-parity placeholders for testport batch b044
//! (`pkg/statistics.part3`: Go test functions 121–180 of
//! `pkg/statistics/**` on `origin/master`, sorted by file path then line).
//!
//! Each `#[ignore]`d test pins one Go `func TestXxx` whose behavior needs a
//! surface this leaf crate deliberately does not own yet (testkit/session SQL
//! harness, statistics Handle, ristretto-backed LFU cache, or Prometheus
//! gauge publishing). The portable members of the batch are already pinned:
//! the five `pkg/statistics/handle/bootstrap_test.go` SQL-generation tests in
//! `bootstrap_sql_source.rs`, and `TestCacheOfBatchUpdate` semantics in
//! `batch_update_source.rs`.

// Gap classes, for readers:
// - session/Handle gap: refresher + worker + ddl + globalstats tests need a
//   testkit session and the statistics Handle runtime this leaf crate does
//   not own.
// - LFU gap: the ristretto-backed stats cache is an external boundary.

// ---- pkg/statistics/handle/autoanalyze/refresher/refresher_test.go ----

#[test]
#[ignore = "go-parity-gap: requires testkit session + statistics Handle runtime not present in tidb-stats"]
fn skip_analyze_table_when_auto_analyze_ratio_is_zero() {
    // Go: refresher_test.go TestSkipAnalyzeTableWhenAutoAnalyzeRatioIsZero
    // Pins that tables stay above the (legacy) zero auto-analyze ratio and
    // are skipped by the refresher.
}

#[test]
#[ignore = "go-parity-gap: requires testkit session + statistics Handle runtime not present in tidb-stats"]
fn ignore_nil_or_pseudo_stats_of_partitioned_table() {
    // Go: refresher_test.go TestIgnoreNilOrPseudoStatsOfPartitionedTable
}

#[test]
#[ignore = "go-parity-gap: requires testkit session + statistics Handle runtime not present in tidb-stats"]
fn ignore_nil_or_pseudo_stats_of_non_partitioned_table() {
    // Go: refresher_test.go TestIgnoreNilOrPseudoStatsOfNonPartitionedTable
}

#[test]
#[ignore = "go-parity-gap: requires testkit session + statistics Handle runtime not present in tidb-stats"]
fn ignore_tiny_table() {
    // Go: refresher_test.go TestIgnoreTinyTable — tables below
    // AutoAnalyzeMinCnt never enter the analysis priority queue.
}

#[test]
#[ignore = "go-parity-gap: requires testkit session + statistics Handle runtime not present in tidb-stats"]
fn analyze_highest_priority_tables() {
    // Go: refresher_test.go TestAnalyzeHighestPriorityTables
}

#[test]
#[ignore = "go-parity-gap: requires testkit session + statistics Handle runtime not present in tidb-stats"]
fn analyze_highest_priority_tables_concurrently() {
    // Go: refresher_test.go TestAnalyzeHighestPriorityTablesConcurrently
}

#[test]
#[ignore = "go-parity-gap: requires testkit session + statistics Handle runtime not present in tidb-stats"]
fn do_not_retry_table_not_exist_job() {
    // Go: refresher_test.go TestDoNotRetryTableNotExistJob
}

#[test]
#[ignore = "go-parity-gap: requires testkit session + statistics Handle runtime not present in tidb-stats"]
fn analyze_highest_priority_tables_with_failed_analysis() {
    // Go: refresher_test.go TestAnalyzeHighestPriorityTablesWithFailedAnalysis
}

// ---- pkg/statistics/handle/autoanalyze/refresher/worker_test.go ----

#[test]
#[ignore = "go-parity-gap: requires testkit session + statistics Handle runtime not present in tidb-stats"]
fn worker_new_admit_and_drain_semantics() {
    // Go: worker_test.go TestWorker — NewWorker capacity, TryAdd/Admit/
    // rejection-at-capacity, and concurrent drain via mockAnalysisJob.
    // Partial primitive coverage lives in worker_capacity_source.rs; the
    // Handle/tracker-backed worker itself is unported.
}

// ---- pkg/statistics/handle/cache/internal/lfu/lfu_cache_test.go ----

#[test]
#[ignore = "go-parity-gap: ristretto-backed LFU cache is an unported external boundary of the stats cache"]
fn lfu_put_get_del() {
    // Go: lfu_cache_test.go TestLFUPutGetDel
}

#[test]
#[ignore = "go-parity-gap: ristretto-backed LFU cache is an unported external boundary of the stats cache"]
fn lfu_fresh_mem_usage() {
    // Go: lfu_cache_test.go TestLFUFreshMemUsage
}

#[test]
#[ignore = "go-parity-gap: ristretto-backed LFU cache is an unported external boundary of the stats cache"]
fn lfu_put_too_big() {
    // Go: lfu_cache_test.go TestLFUPutTooBig
}

#[test]
#[ignore = "go-parity-gap: ristretto-backed LFU cache is an unported external boundary of the stats cache"]
fn cache_len_tracks_entries_despite_eviction() {
    // Go: lfu_cache_test.go TestCacheLen — Len counts list entries even when
    // per-item costs were evicted from the ristretto cache.
}

#[test]
#[ignore = "go-parity-gap: ristretto-backed LFU cache is an unported external boundary of the stats cache"]
fn lfu_cache_put_get_with_many_concurrency() {
    // Go: lfu_cache_test.go TestLFUCachePutGetWithManyConcurrency
}

#[test]
#[ignore = "go-parity-gap: ristretto-backed LFU cache is an unported external boundary of the stats cache"]
fn lfu_cache_put_get_with_many_concurrency2() {
    // Go: lfu_cache_test.go TestLFUCachePutGetWithManyConcurrency2
}

#[test]
#[ignore = "go-parity-gap: ristretto-backed LFU cache is an unported external boundary of the stats cache"]
fn lfu_cache_put_get_with_many_concurrency_and_small_concurrency() {
    // Go: lfu_cache_test.go TestLFUCachePutGetWithManyConcurrencyAndSmallConcurrency
}

#[test]
#[ignore = "go-parity-gap: ristretto-backed LFU cache is an unported external boundary of the stats cache"]
fn lfu_reject() {
    // Go: lfu_cache_test.go TestLFUReject
}

#[test]
#[ignore = "go-parity-gap: ristretto-backed LFU cache is an unported external boundary of the stats cache"]
fn memory_control() {
    // Go: lfu_cache_test.go TestMemoryControl
}

#[test]
#[ignore = "go-parity-gap: ristretto-backed LFU cache is an unported external boundary of the stats cache"]
fn memory_control_with_update() {
    // Go: lfu_cache_test.go TestMemoryControlWithUpdate
}

// ---- pkg/statistics/handle/cache/statscache_test.go ----

#[test]
#[ignore = "go-parity-gap: StatsCacheImpl.UpdateStatsHealthyMetrics gauge publishing not ported; only the bucket catalog (healthy_metrics.rs) exists"]
fn update_stats_healthy_metrics_bucket_distribution() {
    // Go: statscache_test.go TestUpdateStatsHealthyMetrics — eight mock
    // tables must land in buckets {3,1,0,0,0,1,1} plus total/unneeded/pseudo.
    // TestCacheOfBatchUpdate from the same Go file is pinned by
    // batch_update_source.rs.
}

// ---- pkg/statistics/handle/ddl/ddl_test.go ----

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn ddl_after_load() {
    // Go: ddl_test.go TestDDLAfterLoad
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn ddl_table() {
    // Go: ddl_test.go TestDDLTable
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn system_table_ddl_has_no_event() {
    // Go: ddl_test.go TestSystemTableDDLHasNoEvent
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn ddl_truncate_table_updates_modify_count() {
    // Go: ddl_test.go TestTruncateTable
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn ddl_truncate_a_partitioned_table() {
    // Go: ddl_test.go TestTruncateAPartitionedTable
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn ddl_histogram() {
    // Go: ddl_test.go TestDDLHistogram
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn ddl_partition() {
    // Go: ddl_test.go TestDDLPartition
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn reorg_partitions_refresh_stats_meta() {
    // Go: ddl_test.go TestReorgPartitions
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn increase_partition_count_of_hash_partition_table() {
    // Go: ddl_test.go TestIncreasePartitionCountOfHashPartitionTable
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn decrease_partition_count_of_hash_partition_table() {
    // Go: ddl_test.go TestDecreasePartitionCountOfHashPartitionTable
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn truncate_a_partition() {
    // Go: ddl_test.go TestTruncateAPartition
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn truncate_a_partition_and_drop_table_immediately() {
    // Go: ddl_test.go TestTruncateAPartitionAndDropTableImmediately
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn truncate_a_hash_partition() {
    // Go: ddl_test.go TestTruncateAHashPartition
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn truncate_partitions() {
    // Go: ddl_test.go TestTruncatePartitions
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn drop_a_partition() {
    // Go: ddl_test.go TestDropAPartition
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn drop_partitions() {
    // Go: ddl_test.go TestDropPartitions
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn exchange_a_partition() {
    // Go: ddl_test.go TestExchangeAPartition
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn exchange_a_partition_and_drop_table_immediately() {
    // Go: ddl_test.go TestExchangeAPartitionAndDropTableImmediately
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn remove_partitioning_keeps_global_stats() {
    // Go: ddl_test.go TestRemovePartitioning
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn add_partitioning_keeps_global_stats() {
    // Go: ddl_test.go TestAddPartitioning
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn drop_schema_cleans_stats() {
    // Go: ddl_test.go TestDropSchema
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn exchange_partition() {
    // Go: ddl_test.go TestExchangePartition
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn dump_stats_delta_before_handle_ddl_event() {
    // Go: ddl_test.go TestDumpStatsDeltaBeforeHandleDDLEvent
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn dump_stats_delta_before_handle_add_column_event() {
    // Go: ddl_test.go TestDumpStatsDeltaBeforeHandleAddColumnEvent
}

// ---- pkg/statistics/handle/globalstats/global_stats_test.go ----

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn show_global_stats_with_async_merge_global() {
    // Go: global_stats_test.go TestShowGlobalStatsWithAsyncMergeGlobal
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn show_global_stats_without_async_merge_global() {
    // Go: global_stats_test.go TestShowGlobalStatsWithoutAsyncMergeGlobal
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn global_stats_panic_in_io_worker() {
    // Go: global_stats_test.go TestGlobalStatsPanicInIOWorker
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn global_stats_with_cmsketch_err() {
    // Go: global_stats_test.go TestGlobalStatsWithCMSketchErr
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn global_stats_with_histogram_and_topn_err() {
    // Go: global_stats_test.go TestGlobalStatsWithHistogramAndTopNErr
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn global_stats_panic_in_cpu_worker() {
    // Go: global_stats_test.go TestGlobalStatsPanicInCPUWorker
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn global_stats_panic_sametime() {
    // Go: global_stats_test.go TestGlobalStatsPanicSametime
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn global_stats_error_sametime() {
    // Go: global_stats_test.go TestGlobalStatsErrorSametime
}

#[test]
#[ignore = "go-parity-gap: requires testkit SQL harness (session/domain) unavailable in tidb-stats"]
fn build_global_level_stats() {
    // Go: global_stats_test.go TestBuildGlobalLevelStats
}
