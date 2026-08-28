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

//! Gap tests for Go `pkg/executor/executor_failpoint_test.go`. Every retained
//! case coordinates a Go failpoint with a live session, transaction, TiKV
//! client, coprocessor worker, SQL killer, or virtual table. The Rust
//! `tidb-executor` crate has no compatible failpoint registry or session-level
//! execution surface, so these tests are explicit parity gaps rather than
//! approximations.

/// Go `pkg/executor/executor_failpoint_test.go:55::TestTiDBLastTxnInfoCommitMode`.
/// Session transaction settings and the `tidb_last_txn_info` JSON expose
/// async-commit, 1PC, 2PC, and failover choices (`:55-124`).
#[test]
#[ignore = "go-parity-gap: session transaction modes, last-transaction JSON, and TiKV failover failpoints are unported"]
fn tidb_last_txn_info_reports_commit_mode_and_fallbacks() {}

/// Go `pkg/executor/executor_failpoint_test.go:126::TestPointGetRepeatableRead`.
/// Two sessions and two failpoints pin the first point-get read timestamp
/// while another session updates the row (`:126-163`).
#[test]
#[ignore = "go-parity-gap: repeatable-read snapshots, two sessions, and point-get failpoints are unported"]
fn point_get_repeatable_read_keeps_the_first_snapshot() {}

/// Go `pkg/executor/executor_failpoint_test.go:164::TestBatchPointGetRepeatableRead`.
/// The batch point-get variant must likewise retain the first read while a
/// concurrent update changes the unique key (`:164-199`).
#[test]
#[ignore = "go-parity-gap: batch point-get snapshot transactions and failpoint coordination are unported"]
fn batch_point_get_repeatable_read_keeps_the_first_snapshot() {}

/// Go `pkg/executor/executor_failpoint_test.go:200::TestSplitRegionTimeout`.
/// Split-region and scatter failpoints make split commands report timeout
/// counts without blocking partition pre-splitting (`:200-234`).
#[test]
#[ignore = "go-parity-gap: DDL region splitting, scatter timeouts, and TiKV failpoints are unported"]
fn split_region_timeout_reports_partial_split_and_scatter_counts() {}

/// Go `pkg/executor/executor_failpoint_test.go:235::TestTSOFail`.
/// A session TSO failpoint must make a query return an error through the
/// session execution path (`:235-251`).
#[test]
#[ignore = "go-parity-gap: session TSO acquisition and failpoint-driven timestamp errors are unported"]
fn tso_failure_reaches_query_execution() {}

/// Go `pkg/executor/executor_failpoint_test.go:252::TestKillTableReader`.
/// A SQL-killer signal interrupts a retrying table reader and returns the
/// query-interrupted executor error (`:252-279`).
#[test]
#[ignore = "go-parity-gap: SQL killer propagation through a retrying TiKV table reader is unported"]
fn kill_table_reader_interrupts_region_retry() {}

/// Go `pkg/executor/executor_failpoint_test.go:280::TestCollectCopRuntimeStats`.
/// Coprocessor response failpoints must leave `EXPLAIN ANALYZE` runtime text
/// containing RPC and region-miss fields (`:280-295`).
#[test]
#[ignore = "go-parity-gap: TiKV coprocessor runtime statistics and EXPLAIN ANALYZE are unported"]
fn collect_cop_runtime_stats_reports_rpc_and_region_miss() {}

/// Go `pkg/executor/executor_failpoint_test.go:296::TestCoprocessorOOMTiCase` is
/// unconditionally skipped by Go at `:297`; no Rust test is generated.

/// Go `pkg/executor/executor_failpoint_test.go:376::TestCoprocessorBlockIssues56916`.
/// The coprocessor issue failpoint runs indexed reads after table splitting and
/// must not deadlock or lose rows (`:376-394`).
#[test]
#[ignore = "go-parity-gap: coprocessor worker blocking failpoints and split TiKV regions are unported"]
fn coprocessor_block_issue_56916_does_not_lose_rows() {}

/// Go `pkg/executor/executor_failpoint_test.go:395::TestIssue21441`.
/// Union workers with a one-row chunk size must preserve all repeated UNION
/// rows and limits while the union failpoint is enabled (`:395-470`).
#[test]
#[ignore = "go-parity-gap: Go union worker failpoint scheduling and session executor concurrency are unported"]
fn issue_21441_union_workers_preserve_rows_during_shutdown() {}

/// Go `pkg/executor/executor_failpoint_test.go:471::TestUnionExecCloseWaitsForWorkers`.
/// `UnionExec.Close` must wait while a worker is paused and return after the
/// failpoint is disabled (`:471-533`).
#[test]
#[ignore = "go-parity-gap: Go UnionExec worker lifecycle and failpoint pause hooks are unported"]
fn union_exec_close_waits_for_paused_workers() {}

/// Go `pkg/executor/executor_failpoint_test.go:534::TestUnionExecCloseReturnsAfterWorkerPanicDuringShutdown`.
/// Closing a union with a panicking worker must release both `Close` and
/// `Next` after shutdown (`:534-594`).
#[test]
#[ignore = "go-parity-gap: panic-safe Go union worker shutdown is unported"]
fn union_exec_close_returns_after_worker_panic() {}

/// Go `pkg/executor/executor_failpoint_test.go:595::TestTxnWriteThroughputSLI`.
/// Session DML statements update the transaction-write SLI, including small
/// transaction classification, invalid read/write mixes, reset, and failed
/// commit behavior (`:595-685`).
#[test]
#[ignore = "go-parity-gap: session transaction-write SLI accounting and failpoint hooks are unported"]
fn txn_write_throughput_sli_tracks_dml_and_reset() {}

/// Go `pkg/executor/executor_failpoint_test.go:686::TestDeadlocksTable`.
/// The deadlock-history virtual table renders pushed wait-chain records and
/// suppresses digest retrieval under a failpoint (`:686-758`).
#[test]
#[ignore = "go-parity-gap: INFORMATION_SCHEMA deadlocks virtual-table retrieval and Go digest hooks are unported"]
fn deadlocks_table_renders_wait_chain_records() {}

/// Go `pkg/executor/executor_failpoint_test.go:759::TestTiKVClientReadTimeout`.
/// Unistore timeout failpoints must cause point, batch-point, coprocessor,
/// and stale reads to report two-RPC retry details (`:759-851`).
#[test]
#[ignore = "go-parity-gap: TiKV/unistore read-timeout failpoints and runtime RPC details are unported"]
fn tikv_client_read_timeout_retries_point_batch_and_cop_reads() {}

/// Go `pkg/executor/executor_failpoint_test.go:852::TestGetMvccByEncodedKeyRegionError`.
/// An MVCC read must preserve the original commit timestamp after injected
/// epoch-not-match region errors (`:852-879`).
#[test]
#[ignore = "go-parity-gap: MVCC encoded-key helper, unistore region errors, and transaction metadata are unported"]
fn get_mvcc_by_encoded_key_recovers_from_region_error() {}

/// Go `pkg/executor/executor_failpoint_test.go:880::TestShuffleExit`.
/// Shuffle worker and fetch failpoints must surface `ShuffleExec.Next` errors
/// without leaking workers (`:880-902`).
#[test]
#[ignore = "go-parity-gap: Go shuffle worker failpoint/error propagation and session window execution are unported"]
fn shuffle_exit_surfaces_worker_error_without_leak() {}

/// Go `pkg/executor/executor_failpoint_test.go:903::TestHandleForeignKeyCascadePanic`.
/// A foreign-key cascade failpoint must return its injected error rather than
/// leak a goroutine during REPLACE (`:903-921`).
#[test]
#[ignore = "go-parity-gap: foreign-key cascade execution and panic failpoints are unported"]
fn handle_foreign_key_cascade_panic_is_returned() {}

/// Go `pkg/executor/executor_failpoint_test.go:922::TestBuildProjectionForIndexJoinPanic`.
/// The index-join projection-builder failpoint must return its injected error
/// from a generated-column join query (`:922-943`).
#[test]
#[ignore = "go-parity-gap: Go index-join projection builder and panic failpoint are unported"]
fn build_projection_for_index_join_panic_is_returned() {}

/// Go `pkg/executor/executor_failpoint_test.go:1100::TestIndexLookUpPushDownExec`.
/// The Go verifier injects index-lookup handle hit rates and compares pushed
/// down results, limits, and analyzed row counts with a table scan
/// (`:1100-1151`).
#[test]
#[ignore = "go-parity-gap: index-lookup pushdown, handle-filter failpoints, and TiKV execution stats are unported"]
fn index_lookup_pushdown_exec_matches_table_scan() {}

/// Go `pkg/executor/executor_failpoint_test.go:1152::TestIndexLookUpPushDownPartitionExec`.
/// The same verifier covers int, common, and extra handles across four range
/// partitions and hit rates 0, 5, and 10 (`:1152` onward).
#[test]
#[ignore = "go-parity-gap: partitioned index-lookup pushdown and handle-filter failpoints are unported"]
fn index_lookup_pushdown_partition_exec_matches_table_scan() {}
