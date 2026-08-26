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

//! Batch b047 (`pkg/statistics.part6`) — Go unit tests from
//! `pkg/statistics/handle/lockstats` (query/unlock), `pkg/statistics/handle/storage`
//! (dump/gc/read/stats_read_writer), `pkg/statistics/handle/syncload`,
//! `pkg/statistics/handle/updatetest`, read from `origin/master`.
//!
//! Almost every test in this slice drives the statistics handle through a mock
//! store / session (`testkit.CreateMockStoreAndDomain` or a mocked
//! `RestrictedSQLExecutor`). Those behaviors are marked `#[ignore]` with a
//! `go-parity-gap` reason until the owning Rust surface exists; they are never
//! approximated. The pure slices that ARE ported are pinned elsewhere:
//! - `TestGenerateSkippedTablesMessage` / `TestGenerateSkippedPartitionsMessage`
//!   → `lock_messages_source.rs`
//! - `TestGetTablesLockedStatuses` → `locked_tables_source.rs`
//! - `TestGetStatsDeltaFromTableLocked` → `stats_delta_source.rs`
//! - slow-stats-saving lease-threshold decisions → `stats_read_writer_source.rs`
//! - GC `forCount` batching → `gc_batch_count_source.rs`

use tidb_stats::gc_batch_count;

// --- pkg/statistics/handle/lockstats/query_lock_test.go ---

// TestGetTablesLockedStatuses' pure filtering slice is pinned exactly by
// locked_tables_source::source_locked_table_filter_returns_only_requested_locked_ids
// and ::source_locked_table_filter_empty_locked_set_is_empty.

#[ignore]
#[test]
fn query_locked_tables_session_execution() {
    // go-parity-gap: requires the mocked RestrictedSQLExecutor/session layer.
    unimplemented!("session-bound Go test")
}

// --- pkg/statistics/handle/lockstats/unlock_stats_test.go ---

#[test]
fn source_unlock_stats_pure_slice_is_pinned_elsewhere() {
    // The row-shape rule of TestGetStatsDeltaFromTableLocked (first row →
    // (count, modify_count), empty result → zeros, error passthrough) is pinned
    // by stats_delta_source::source_empty_delta_rows_use_zero_values,
    // source_delta_rows_read_the_first_count_and_modify_count, and
    // source_delta_query_errors_are_returned_unchanged. Re-derive one case here
    // via the same public API to keep the Go case visible in this batch.
    let rows = [(1_i64, 1_i64)];
    assert_eq!(
        tidb_stats::stats_delta_from_rows::<&str>(Ok(&rows)).map(|d| (d.count, d.modify_count)),
        Ok((1, 1))
    );
}

#[ignore]
#[test]
fn update_stats_and_unlock_table() {
    // go-parity-gap: failpoint-guarded UPDATE/DELETE against mysql tables via
    // session executor; not ported to tidb-stats.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn remove_locked_tables() {
    // go-parity-gap: DELETE FROM mysql.stats_table_locked driven through the
    // session executor; not ported to tidb-stats.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn remove_locked_partitions() {
    // go-parity-gap: partition-scoped unlock SQL via session executor.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn remove_locked_partitions_failed_if_the_whole_table_is_locked() {
    // go-parity-gap: whole-table-lock rejection path needs the storage-backed
    // lock lookup.
    unimplemented!("storage-bound Go test")
}

// --- pkg/statistics/handle/main_test.go, updatetest/main_test.go ---
// TestMain harnesses (testkit bootstrap) — skipped-reason: Go test harness,
// not an assertion-bearing test.

// --- pkg/statistics/handle/storage/dump_test.go ---

#[ignore]
#[test]
fn conversion_dump_stats_round_trip() {
    // go-parity-gap: TestConversion drives DumpStatsToJSON /
    // LoadStatsFromJSON over a mock-store domain.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn dump_global_stats() {
    // go-parity-gap: needs handle + storage + session ANALYZE.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn load_global_stats() {
    // go-parity-gap: needs handle + storage + session ANALYZE.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn last_stats_hist_update_version_after_load_stats() {
    // go-parity-gap: LastStatsHistUpdateVersion observed through storage loads.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn load_partition_stats() {
    // go-parity-gap: partition stats loading via handle/mock store.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn load_predicate_columns() {
    // go-parity-gap: predicate-column persistence reads via storage.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn load_partition_stats_err_panic() {
    // go-parity-gap: panic-recovery behavior of the storage loader.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn dump_partitions() {
    // go-parity-gap: DumpStatsToJSON partition walk over a mock domain.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn dump_altered_table() {
    // go-parity-gap: DDL-altered table dump via handle + infoschema.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn dump_pseudo_columns() {
    // go-parity-gap: pseudo-column rendering in DumpStatsToJSON.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn dump_ver2_stats() {
    // go-parity-gap: analyze-version-2 JSON dump via handle.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn load_stats_for_new_collation() {
    // go-parity-gap: collation-aware histogram decode via storage.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn json_table_to_blocks() {
    // go-parity-gap: JSONTableToBlocks/BlocksToJSONTable round trip needs a
    // dumped JSONTable from a live handle; tidb-stats has json_metadata leaves
    // but no block chunking port.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn load_stats_from_old_version() {
    // go-parity-gap: old-format stats upgrade path via storage reads.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn persist_stats() {
    // go-parity-gap: SaveStatsToStorage write path via handle.
    unimplemented!("storage-bound Go test")
}

// --- pkg/statistics/handle/storage/gc_test.go ---

#[test]
fn source_gc_forcount_batching_matches_go_arithmetic() {
    // gc_test.go exercises GCStats whose loop batching helper forCount is the
    // pure slice ported as gc_batch_count; pin its arithmetic here too so the
    // Go relationship stays visible in this batch.
    assert_eq!(gc_batch_count(0, 10), 0);
    assert_eq!(gc_batch_count(1, 10), 1);
    assert_eq!(gc_batch_count(10, 10), 1);
    assert_eq!(gc_batch_count(11, 10), 2);
}

#[ignore]
#[test]
fn gc_stats() {
    // go-parity-gap: GCStats walks mysql.* tables through the session executor.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn gc_partition() {
    // go-parity-gap: partition GC via DDL-dropped physical IDs in storage.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn gc_column_stats_usage() {
    // go-parity-gap: mysql.column_stats_usage retention window via storage.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn delete_analyze_jobs() {
    // go-parity-gap: mysql.analyze_jobs retention deletion via storage.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn extrem_case_of_gc() {
    // go-parity-gap: extreme-ID GC regression through the storage layer.
    unimplemented!("storage-bound Go test")
}

// --- pkg/statistics/handle/storage/read_test.go ---

#[ignore]
#[test]
fn load_stats() {
    // go-parity-gap: LoadStatsHistograms/LoadNeededHistograms via mock store.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn load_non_existent_index_stats() {
    // go-parity-gap: missing-index load path via storage.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn column_stats_is_invalid_skips_internal_column_id() {
    // go-parity-gap: internal-column-ID guard exercised through stored stats.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn load_needed_histograms_skips_internal_column_id() {
    // go-parity-gap: internal-column-ID guard in LoadNeededHistograms.
    unimplemented!("storage-bound Go test")
}

// --- pkg/statistics/handle/storage/stats_read_writer_test.go ---

#[ignore]
#[test]
fn update_stats_meta_version_for_gc() {
    // go-parity-gap: UPDATE mysql.stats_meta version bump verified against
    // storage + history table.
    unimplemented!("storage-bound Go test")
}

// TestSlowStatsSaving / TestSlowStatsSavingForPartitionedTable /
// TestFailedToHandleSlowStatsSaving: their lease-threshold decision slices are
// pinned by stats_read_writer_source (positive five-minute lease, non-positive
// lease disabled with force override, source error text). The full Go tests
// remain storage-bound:

#[ignore]
#[test]
fn slow_stats_saving() {
    // go-parity-gap: async save-worker timing measured via mock store.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn slow_stats_saving_for_partitioned_table() {
    // go-parity-gap: partitioned-table save-worker timing via mock store.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn failed_to_handle_slow_stats_saving() {
    // go-parity-gap: failure-injection path of the save worker.
    unimplemented!("storage-bound Go test")
}

// --- pkg/statistics/handle/syncload/stats_syncload_test.go ---

#[ignore]
#[test]
fn concurrent_load_hist() {
    // go-parity-gap: sync-load worker pool via session + storage.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn concurrent_load_hist_timeout() {
    // go-parity-gap: sync-load timeout path via session + storage.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn concurrent_load_hist_with_panic_and_fail() {
    // go-parity-gap: panic/failure injection into sync-load workers.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn retry() {
    // go-parity-gap: sync-load retry policy via session + storage.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn send_load_requests_wait_too_long() {
    // go-parity-gap: request-timeout branch of SendLoadRequests.
    unimplemented!("storage-bound Go test")
}

#[ignore]
#[test]
fn sync_load_on_object_which_can_not_found_in_storage() {
    // go-parity-gap: missing-object sync-load path via storage.
    unimplemented!("storage-bound Go test")
}

// --- pkg/statistics/handle/updatetest/update_test.go ---

#[ignore]
#[test]
fn single_session_insert() {
    // go-parity-gap: stats-meta delta maintenance after INSERT via session.
    unimplemented!("session-bound Go test")
}

#[ignore]
#[test]
fn rollback() {
    // go-parity-gap: transaction rollback delta behavior via session.
    unimplemented!("session-bound Go test")
}

#[ignore]
#[test]
fn multi_session() {
    // go-parity-gap: cross-session delta merge via session.
    unimplemented!("session-bound Go test")
}

#[ignore]
#[test]
fn txn_with_failure() {
    // go-parity-gap: failed-transaction delta behavior via session.
    unimplemented!("session-bound Go test")
}

#[ignore]
#[test]
fn update_partition() {
    // go-parity-gap: partition-level Update via handle + storage.
    unimplemented!("session-bound Go test")
}

#[ignore]
#[test]
fn auto_update() {
    // go-parity-gap: auto-analyze feedback update loop via session.
    unimplemented!("session-bound Go test")
}

#[ignore]
#[test]
fn auto_update_partition() {
    // go-parity-gap: partitioned auto-update loop via session.
    unimplemented!("session-bound Go test")
}

#[ignore]
#[test]
fn issue25700() {
    // go-parity-gap: regression needing session-driven UPDATE + analyze.
    unimplemented!("session-bound Go test")
}

#[ignore]
#[test]
fn split_range() {
    // go-parity-gap: Histogram.SplitRange needs ranger.Range + stmtctx, which
    // tidb-stats does not depend on yet.
    unimplemented!("ranger-dependent Go test")
}

#[ignore]
#[test]
fn out_of_order_update() {
    // go-parity-gap: out-of-order stats-meta updates via session.
    unimplemented!("session-bound Go test")
}

#[ignore]
#[test]
fn load_hist_correlation() {
    // go-parity-gap: column correlation reload via storage.
    unimplemented!("session-bound Go test")
}

// BenchmarkHandleAutoAnalyze — skipped-reason: throughput benchmark without
// assertions.

#[ignore]
#[test]
fn merge_top_n() {
    // go-parity-gap: merge-partition TopN during auto-update via handle.
    unimplemented!("session-bound Go test")
}

#[ignore]
#[test]
fn stats_variables() {
    // go-parity-gap: session-variable effects on stats update via session.
    unimplemented!("session-bound Go test")
}

#[ignore]
#[test]
fn auto_update_partition_in_dynamic_only_mode() {
    // go-parity-gap: dynamic-prune-mode auto update via session.
    unimplemented!("session-bound Go test")
}

#[ignore]
#[test]
fn auto_analyze_ratio() {
    // go-parity-gap: auto-analyze ratio trigger evaluated on a live session.
    unimplemented!("session-bound Go test")
}
