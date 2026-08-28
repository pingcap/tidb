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

//! Ports of `pkg/executor.part23`: deterministic items 1321--1380 of the
//! upstream executor test enumeration. The running rows use only the
//! in-memory catalog and executor seams owned by this crate. Rows requiring
//! session transactions, TiKV/PD, TiProxy, LOAD DATA, runtime memory
//! measurements, or Go-private helpers remain explicit parity gaps.

use crate::{
    Catalog, StmtContext, run_create_table_on, run_insert_on, run_insert_reporting, run_select_on,
};
use tidb_datatype::Datum;

fn query_ctx() -> StmtContext {
    StmtContext::for_query()
}

fn dml_ctx() -> StmtContext {
    StmtContext::for_dml(false, true, false)
}

fn int_value(datum: &Datum) -> i64 {
    match datum {
        Datum::Int(value) => *value,
        Datum::UInt(value) => i64::try_from(*value).expect("test value fits in i64"),
        other => panic!("expected integer datum, got {other:?}"),
    }
}

/// Go `pkg/executor/test/txn/txn_test.go:34::TestInvalidReadTemporaryTable`.
#[test]
#[ignore = "go-parity-gap: global/local temporary tables, stale reads, and tidb_snapshot are session and DDL surfaces not exposed by tidb-executor"]
fn invalid_read_temporary_table_part23() {}

/// Go `pkg/executor/test/txn/txn_test.go:156::TestInvalidReadCacheTable`.
#[test]
#[ignore = "go-parity-gap: ALTER TABLE CACHE and stale reads of cache tables require session metadata and storage state"]
fn invalid_read_cache_table_part23() {}

/// Go `pkg/executor/test/txn/txn_test.go:259::TestTxnSavepoint0`.
#[test]
#[ignore = "go-parity-gap: SAVEPOINT stack state belongs to tidb-session; the executor catalog driver has no transaction statement surface"]
fn txn_savepoint0_stack_contract_part23() {}

/// Go `pkg/executor/test/txn/txn_test.go:340::TestTxnSavepoint1`.
#[test]
#[ignore = "go-parity-gap: explicit transactions, rollback-to-savepoint data visibility, and session transaction modes are unported"]
fn txn_savepoint1_data_semantics_part23() {}

/// Go `pkg/executor/test/txn/txn_test.go:469::TestRollbackToSavepointReleasePessimisticLock`.
#[test]
#[ignore = "go-parity-gap: cross-session pessimistic locks and lock release after rollback are unported"]
fn rollback_to_savepoint_release_pessimistic_lock_part23() {}

/// Go `pkg/executor/test/txn/txn_test.go:515::TestSavepointInPessimisticAndOptimistic`.
#[test]
#[ignore = "go-parity-gap: mixed-mode multi-session transaction visibility is not exposed by this crate"]
fn savepoint_in_pessimistic_and_optimistic_part23() {}

/// Go `pkg/executor/test/txn/txn_test.go:556::TestSavepointInBigTxn`.
#[test]
#[ignore = "go-parity-gap: 10,000-statement transaction/savepoint rollback behavior requires the session transaction layer"]
fn savepoint_in_big_txn_part23() {}

/// Go `pkg/executor/test/txn/txn_test.go:637::TestSavepointWithCacheTable`.
#[test]
#[ignore = "go-parity-gap: cached-table transaction state and SAVEPOINT rollback are unported"]
fn savepoint_with_cache_table_part23() {}

/// Go `pkg/executor/test/txn/txn_test.go:677::TestColumnNotMatchError`.
#[test]
#[ignore = "go-parity-gap: DDL reorg failpoints and commit-time schema verification are unported; Go skips this on next-gen kernels"]
fn column_not_match_error_part23() {}

/// Go `pkg/executor/test/txn/txn_test.go:716::TestSavepointWithForeignKey`.
#[test]
#[ignore = "go-parity-gap: foreign-key locks across savepoint rollback and multiple sessions are unported"]
fn savepoint_with_foreign_key_part23() {}

/// Go `pkg/executor/test/txn/txn_test.go:767::TestInnodbLockWaitTimeout`.
#[test]
#[ignore = "go-parity-gap: innodb_lock_wait_timeout, injected lock conflicts, and pessimistic lock waits are unported"]
fn innodb_lock_wait_timeout_part23() {}

/// Go `pkg/executor/test/unstabletest/main_test.go:27::TestMain`.
#[test]
#[ignore = "skipped-reason: Go suite configuration and goleak bootstrap have no Rust product behavior"]
fn unstabletest_main_is_bootstrap_only_part23() {}

/// Go `pkg/executor/test/unstabletest/memory_test.go:35::TestGlobalMemoryControl`.
#[test]
#[ignore = "go-parity-gap: instance ServerMemoryLimitHandle scheduling, process memory accounting, and top-session cancellation are unported"]
fn global_memory_control_kills_the_top_consumer_part23() {}

/// Go `pkg/executor/test/unstabletest/memory_test.go:105::TestPBMemoryLeak`.
#[test]
#[ignore = "go-parity-gap: runtime.MemStats, forced GC, a 256 MiB table, and record-set allocation measurements are not exposed by tidb-executor"]
fn pb_memory_leak_part23() {}

/// Go `pkg/executor/test/writetest/main_test.go:27::TestMain`.
#[test]
#[ignore = "skipped-reason: Go suite configuration and goleak bootstrap have no Rust product behavior"]
fn writetest_main_is_bootstrap_only_part23() {}

/// Go `pkg/executor/test/writetest/write_test.go:42::TestInsertIgnore`.
#[test]
fn insert_ignore_keeps_the_nonduplicate_rows_part23() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (id int primary key, c int unique key)",
        &mut catalog,
    )
    .expect("table creates");
    run_insert_on("insert into t values (1, 2)", &mut catalog, &dml_ctx()).expect("seed insert");
    run_insert_reporting(
        "insert ignore into t values (1, 3), (2, 3)",
        &mut catalog,
        "test",
        &dml_ctx(),
    )
    .expect("insert ignore succeeds");

    let rows = run_select_on("select id, c from t order by id", &catalog, &query_ctx())
        .expect("select succeeds");
    assert_eq!(rows.len(), 2);
    assert_eq!(int_value(&rows[0][0]), 1);
    assert_eq!(int_value(&rows[0][1]), 2);
    assert_eq!(int_value(&rows[1][0]), 2);
    assert_eq!(int_value(&rows[1][1]), 3);
}

/// Go `pkg/executor/test/writetest/write_test.go:199::TestLoadDataMissingColumn`.
#[test]
#[ignore = "go-parity-gap: LOAD DATA reader injection, timestamp defaults, warning counts, and row counters are unported"]
fn load_data_missing_column_uses_timestamp_default_part23() {}

/// Go `pkg/executor/test/writetest/write_test.go:231::TestIssue18681`.
#[test]
#[ignore = "go-parity-gap: LOAD DATA bit-field conversion and warning-level type flags are unported"]
fn issue18681_load_data_bit_columns_follow_non_strict_conversion_part23() {}

/// Go `pkg/executor/test/writetest/write_test.go:261::TestIssue34358`.
#[test]
#[ignore = "go-parity-gap: LOAD DATA user-variable assignment and reader injection are unported"]
fn issue34358_load_data_user_variables_accept_null_literals_part23() {}

/// Go `pkg/executor/test/writetest/write_test.go:279::TestLatch`.
#[test]
#[ignore = "go-parity-gap: concurrent pessimistic transactions and TiDB latch conflict/retry behavior are unported"]
fn latch_allows_disjoint_writes_and_retries_conflicts_part23() {}

/// Go `pkg/executor/test/writetest/write_test.go:334::TestReplaceLog`.
#[test]
#[ignore = "go-parity-gap: raw index corruption, REPLACE repair, and ADMIN CLEANUP INDEX require the session/storage stack"]
fn replace_reports_a_dangling_index_row_part23() {}

/// Go `pkg/executor/test/writetest/write_test.go:368::TestRebaseIfNeeded`.
#[test]
#[ignore = "go-parity-gap: direct table AddRecord, auto-ID rebasing, and session transaction state are unported"]
fn rebase_if_needed_does_not_rebase_unchanged_updates_part23() {}

/// Go `pkg/executor/test/writetest/write_test.go:400::TestDeferConstraintCheckForInsert`.
#[test]
#[ignore = "go-parity-gap: deferred constraint checking, autocommit, temporary tables, and transaction rollback are unported"]
fn defer_constraint_check_for_insert_checks_at_the_configured_phase_part23() {}

/// Go `pkg/executor/test/writetest/write_test.go:527::TestPessimisticDeleteYourWrites`.
#[test]
#[ignore = "go-parity-gap: two-session pessimistic locking and delete-your-writes visibility are unported"]
fn pessimistic_delete_your_writes_unblocks_the_second_insert_part23() {}

/// Go `pkg/executor/tikv_regions_peers_table_test.go:89::TestTikvRegionPeers`.
#[test]
#[ignore = "go-parity-gap: PD HTTP mocks, TiKV region metadata, and the TIKV_REGION_PEERS infoschema table are unported"]
fn tikv_region_peers_filters_mock_pd_rows_part23() {}

/// Go `pkg/executor/trace_test.go:24::TestTraceExec`.
#[test]
#[ignore = "go-parity-gap: TRACE execution, runtime trace spans, and trace result formatting are session-owned"]
fn trace_exec_returns_ordered_trace_rows_part23() {}

/// Go `pkg/executor/traffic_test.go:51::TestTrafficForm`.
#[test]
#[ignore = "go-parity-gap: TRAFFIC executors, TiProxy HTTP fan-out, and external-storage paths are unported"]
fn traffic_form_maps_capture_and_replay_options_part23() {}

/// Go `pkg/executor/traffic_test.go:141::TestTrafficError`.
#[test]
#[ignore = "go-parity-gap: TRAFFIC error propagation needs TiProxy and object-storage clients"]
fn traffic_errors_surface_proxy_and_storage_failures_part23() {}

/// Go `pkg/executor/traffic_test.go:174::TestCapturePath`.
#[test]
#[ignore = "go-parity-gap: capture path fan-out across multiple TiProxy servers is unported"]
fn traffic_capture_assigns_one_path_per_proxy_part23() {}

/// Go `pkg/executor/traffic_test.go:213::TestReplayPath`.
#[test]
#[ignore = "go-parity-gap: replay manifest discovery and object-storage enumeration are unported"]
fn traffic_replay_selects_proxy_input_paths_part23() {}

/// Go `pkg/executor/traffic_test.go:304::TestTrafficShow`.
#[test]
#[ignore = "go-parity-gap: SHOW TRAFFIC jobs and TiProxy job JSON decoding are unported"]
fn traffic_show_formats_capture_and_replay_jobs_part23() {}

/// Go `pkg/executor/traffic_test.go:400::TestTrafficPrivilege`.
#[test]
#[ignore = "go-parity-gap: TRAFFIC privilege checks and session privilege managers are unported"]
fn traffic_privilege_filters_capture_and_replay_jobs_part23() {}

/// Go `pkg/executor/union_scan_test.go:31::TestUnionScanForMemBufferReader`.
#[test]
#[ignore = "go-parity-gap: transaction mem-buffer union scans, index readers, and ADMIN CHECK TABLE are not exposed by the catalog gateway"]
fn union_scan_merges_dirty_rows_for_table_and_index_reads_part23() {}

/// Go `pkg/executor/union_scan_test.go:186::TestIssue53951`.
#[test]
#[ignore = "go-parity-gap: generated-column index scans with transactional updates require the session transaction layer"]
fn issue53951_union_scan_filters_a_transactional_generated_column_part23() {}

/// Go `pkg/executor/union_scan_test.go:251::TestIssue28073`.
#[test]
#[ignore = "go-parity-gap: pessimistic FOR UPDATE locks and raw MVCC key inspection are unported"]
fn issue28073_does_not_write_partition_id_zero_keys_part23() {}

/// Go `pkg/executor/union_scan_test.go:309::TestIssue32422`.
#[test]
#[ignore = "go-parity-gap: cached tables, asynchronous cache readiness, and session ReadFromTableCache state are unported"]
fn issue32422_union_scan_reads_cached_table_rows_part23() {}

/// Go `pkg/executor/union_scan_test.go:363::TestSnapshotWithConcurrentWrite`.
#[test]
#[ignore = "go-parity-gap: a 524288-row transaction snapshot and concurrent-write MVCC behavior are unported"]
fn snapshot_with_concurrent_write_is_consistent_part23() {}

/// Go `pkg/executor/union_scan_test.go:378::BenchmarkUnionScanRead`.
#[test]
#[ignore = "skipped-reason: Go benchmark; performance and allocation measurements are not unit-test parity claims"]
fn benchmark_union_scan_read_part23() {}

/// Go `pkg/executor/union_scan_test.go:404::BenchmarkUnionScanIndexReadDescRead`.
#[test]
#[ignore = "skipped-reason: Go benchmark; performance and allocation measurements are not unit-test parity claims"]
fn benchmark_union_scan_index_read_desc_read_part23() {}

/// Go `pkg/executor/union_scan_test.go:427::BenchmarkUnionScanTableReadDescRead`.
#[test]
#[ignore = "skipped-reason: Go benchmark; performance and allocation measurements are not unit-test parity claims"]
fn benchmark_union_scan_table_read_desc_read_part23() {}

/// Go `pkg/executor/union_scan_test.go:450::BenchmarkUnionScanIndexLookUpDescRead`.
#[test]
#[ignore = "skipped-reason: Go benchmark; performance and allocation measurements are not unit-test parity claims"]
fn benchmark_union_scan_index_lookup_desc_read_part23() {}

/// Go `pkg/executor/union_scan_test.go:473::TestBenchDaily`.
#[test]
#[ignore = "skipped-reason: benchmark aggregation carrier; the referenced Go benchmarks are out of unit-test scope"]
fn test_bench_daily_is_benchmark_bootstrap_part23() {}

/// Go `pkg/executor/update_test.go:32::TestPessimisticUpdatePKLazyCheck`.
#[test]
#[ignore = "go-parity-gap: pessimistic transactions, clustered-index modes, and presume-key flags are unported"]
fn pessimistic_update_pk_lazy_check_tracks_presume_exists_keys_part23() {}

/// Go `pkg/executor/update_test.go:74::TestLockUnchangedUniqueKeys`.
#[test]
#[ignore = "go-parity-gap: lock_unchanged_keys, concurrent sessions, and blocking lock timing are unported"]
fn lock_unchanged_unique_keys_gates_the_second_writer_part23() {}

/// Go `pkg/executor/update_test.go:197::TestLockUnchangedKeysGlobalIndex`.
#[test]
#[ignore = "go-parity-gap: global-index partition handles, pessimistic locking, and ADMIN CHECK TABLE are unported"]
fn lock_unchanged_keys_global_index_handles_nulls_part23() {}

/// Go `pkg/executor/update_test.go:232::TestUpdateRowRetryAndThenDupKey`.
#[test]
#[ignore = "go-parity-gap: stepped executor breakpoints, optimistic retry, and concurrent duplicate-key writes are unported"]
fn update_row_retry_then_duplicate_key_preserves_the_original_row_part23() {}

/// Go `pkg/executor/update_test.go:263::TestUpdateWithOnUpdateAndAutoGenerated`.
#[test]
#[ignore = "go-parity-gap: session clock, ON UPDATE generated chains, index/table consistency, and fast table check are unported"]
fn update_with_on_update_and_auto_generated_columns_stays_consistent_part23() {}

/// Go `pkg/executor/utils_test.go:34::TestBatchRetrieverHelper`.
#[test]
#[ignore = "go-parity-gap: batchRetrieverHelper is a Go-private infoschema retriever helper with no Rust counterpart"]
fn batch_retriever_helper_emits_contiguous_ranges_part23() {}

/// Go `pkg/executor/utils_test.go:107::TestEqualDatumsAsBinary`.
#[test]
#[ignore = "go-parity-gap: equalDatumsAsBinary is a Go-private InsertValues helper and has no Rust public seam"]
fn equal_datums_as_binary_compares_rows_without_coercion_part23() {}

/// Go `pkg/executor/utils_test.go:143::TestEncodePasswordWithPlugin`.
#[test]
#[ignore = "go-parity-gap: encodePasswordWithPlugin is a Go-private account executor helper; authentication plugins live above this crate"]
fn encode_password_with_plugin_delegates_to_the_auth_plugin_part23() {}

/// Go `pkg/executor/utils_test.go:184::TestWorkerPool`.
#[test]
fn worker_pool_preserves_the_go_submission_contract_part23() {
    let values: Vec<usize> = (0..16).collect();
    let output = crate::worker_pool::map(
        values.iter().map(|value| {
            let value = *value;
            move || {
                if value % 3 == 0 {
                    std::thread::sleep(std::time::Duration::from_micros(50));
                }
                value * 2
            }
        }),
        2,
    );
    assert_eq!(
        output,
        values.iter().map(|value| value * 2).collect::<Vec<_>>()
    );
}

/// Go `pkg/executor/utils_test.go:300::TestEncodedPassword`.
#[test]
#[ignore = "go-parity-gap: encodedPassword is a Go-private account helper and password storage belongs to tidb-session"]
fn encoded_password_validates_plugin_specific_hashes_part23() {}

/// Go `pkg/executor/windows/window_executor_test.go:38::TestWindowExecutorsBasic`.
#[test]
fn window_executor_basic_row_number_and_sum_match_go_part23() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t (a int, b int)", &mut catalog).expect("table creates");
    run_insert_on(
        "insert into t values (1,1),(1,2),(2,1),(2,2)",
        &mut catalog,
        &dml_ctx(),
    )
    .expect("rows insert");
    let rows = run_select_on(
        "select a, row_number() over(partition by a order by b) from t order by a, b",
        &catalog,
        &query_ctx(),
    )
    .expect("window query succeeds");
    let values: Vec<(i64, i64)> = rows
        .iter()
        .map(|row| (int_value(&row[0]), int_value(&row[1])))
        .collect();
    assert_eq!(values, [(1, 1), (1, 2), (2, 1), (2, 2)]);
}

/// Go `pkg/executor/windows/window_executor_test.go:59::TestBuildOrderedWindowExec`.
#[test]
#[ignore = "go-parity-gap: the Go mock data source and direct ordered-window executor builder are private executor seams"]
fn build_ordered_window_exec_consumes_partitioned_mock_chunks_part23() {}

/// Go `pkg/executor/windows/window_executor_test.go:132::TestWindowReturnColumnNullableAttribute`.
#[test]
#[ignore = "go-parity-gap: result-field nullability metadata is exposed through tidb-session, not this catalog gateway"]
fn window_return_column_nullable_attribute_matches_function_contracts_part23() {}

/// Go `pkg/executor/windows/window_sql_test.go:26::TestWindowFunctions`.
#[test]
#[ignore = "go-parity-gap: the full SQL window matrix depends on session window variables and parallel/pipelined execution controls"]
fn window_functions_cover_parallel_and_pipelined_modes_part23() {}

/// Go `pkg/executor/windows/window_sql_test.go:216::TestWindowFunctionsDataReference`.
#[test]
#[ignore = "go-parity-gap: session chunk sizing and the complete window data-reference matrix are unported"]
fn window_functions_data_reference_survives_chunk_boundaries_part23() {}

/// Go `pkg/executor/windows/window_sql_test.go:240::TestSlidingWindowFunctions`.
#[test]
#[ignore = "go-parity-gap: prepared frame parameters, session precision variables, and sliding window families are unported"]
fn sliding_window_functions_cover_rows_range_and_precision_modes_part23() {}

/// Go `pkg/executor/windows/window_sql_test.go:439::TestIssue45964And46050`.
#[test]
#[ignore = "go-parity-gap: result-field nullability for all window functions is a session metadata surface"]
fn issue45964_and_46050_window_nullability_is_precise_part23() {}

/// Go `pkg/executor/windows/window_sql_test.go:474::TestVarSampAsAWindowFunction`.
#[test]
#[ignore = "go-parity-gap: VAR_SAMP window registration and empty-table session execution are not exposed here"]
fn var_samp_is_accepted_as_a_window_function_part23() {}
