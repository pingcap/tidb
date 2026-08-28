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

//! Remaining `pkg/executor/executor_failpoint_test.go` tests whose behavior
//! this tier cannot pin. Each ignored test names the Go test, the Go symbols
//! it exercises, and the missing seam. `TestCoprocessorOOMTiCase`
//! (pkg/executor/executor_failpoint_test.go:297) is skipped in Go itself
//! (`t.Skip("skip")`) and therefore has no Rust counterpart at all.

/// Go `pkg/executor/executor_failpoint_test.go:280::TestCollectCopRuntimeStats`.
// go-parity-gap: pins `explain analyze` cop-runtime stats rendering
// ("num_rpc:..., regionMiss:..." via `tikvclient/tikvStoreRespResult`
// failpoint); this tier's EXPLAIN ANALYZE prints N/A execution info
// (`crate::explain` documents the omission) and has no cop-client stats or
// failpoint tier.
#[test]
#[ignore]
fn collect_cop_runtime_stats_renders_num_rpc_and_region_miss() {}

/// Go `pkg/executor/executor_failpoint_test.go:376::TestCoprocessorBlockIssues56916`.
// go-parity-gap: needs the `pkg/store/copr/issue56916` failpoint that blocks
// coprocessor responses against a split mock store; this tier has no
// store-side coprocessor failpoints and no region splitting.
#[test]
#[ignore]
fn coprocessor_blocking_issue56916_keeps_cooldown_reads_correct() {}

/// Go `pkg/executor/executor_failpoint_test.go:471::TestUnionExecCloseWaitsForWorkers`.
// go-parity-gap: Go's `unionexec.UnionExec` runs per-child fetch workers and
// `Close` must join them (`pkg/executor/unionexec/union.go`); the test pauses
// a worker through the `pauseUnionExecResultPuller` failpoint and asserts
// Close/Next blocking across goroutines. This tier executes set operations
// synchronously in the driver with no worker pool to join.
#[test]
#[ignore]
fn union_exec_close_waits_for_paused_workers() {}

/// Go `pkg/executor/executor_failpoint_test.go:534::
/// TestUnionExecCloseReturnsAfterWorkerPanicDuringShutdown`.
// go-parity-gap: the worker-panic-during-Close handshake
// (`unionPanicExec` + goroutine channels) has no counterpart: this tier has
// no union worker goroutines whose panic path Close must absorb.
#[test]
#[ignore]
fn union_exec_close_returns_after_worker_panic_during_shutdown() {}

/// Go `pkg/executor/executor_failpoint_test.go:595::TestTxnWriteThroughputSLI`.
// go-parity-gap: pins `pkg/util/sli`'s TxnWriteThroughputSLI accounting
// (writeSize/readKeys/writeKeys per statement, small-txn classification,
// invalidation on `insert...select`/`replace...select`, clean-up of failed
// commits, driven by the `sli/CheckTxnWriteThroughput` failpoint); the SLI
// metrics surface is unported in this tier.
#[test]
#[ignore]
fn txn_write_throughput_sli_accounts_statements() {}

/// Go `pkg/executor/executor_failpoint_test.go:759::TestTiKVClientReadTimeout`.
// go-parity-gap: needs the `unistore/unistoreRPCDeadlineExceeded` failpoint to
// force RPC retries and pins `num_rpc:2` in EXPLAIN ANALYZE runtime columns
// for Point_Get/Batch_Point_Get/TableReader plus stale reads; none of the
// retry counter rendering, the store failpoint, or stale-read
// `closest-replica` machinery is ported.
#[test]
#[ignore]
fn tikv_client_read_timeout_retries_show_num_rpc_two() {}

/// Go `pkg/executor/executor_failpoint_test.go:852::TestGetMvccByEncodedKeyRegionError`.
// go-parity-gap: `helper.GetMvccByEncodedKey` MVCC trace lookups with the
// `unistore/epochNotMatch` failpoint retry path; this tier has no MVCC
// get-by-key trace API nor region-epoch retry simulation.
#[test]
#[ignore]
fn get_mvcc_by_encoded_key_survives_region_epoch_errors() {}

/// Go `pkg/executor/executor_failpoint_test.go:880::TestShuffleExit`.
// go-parity-gap: Go injects `shuffleError`, `shuffleExecFetchDataAndSplit`
// and `shuffleWorkerRun` (panic) failpoints into `ShuffleExec` and requires
// the error to surface as "ShuffleExec.Next error"; the Rust shuffle operator
// (`crate::shuffle`) has no failpoint seams and no worker-panic propagation
// contract.
#[test]
#[ignore]
fn shuffle_exit_propagates_worker_panics_as_next_errors() {}

/// Go `pkg/executor/executor_failpoint_test.go:903::TestHandleForeignKeyCascadePanic`.
// go-parity-gap: the FK cascade error injection
// (`handleForeignKeyCascadeError` failpoint over `replace` cascade execution)
// requires the foreign-key cascade executor path; this tier ports no cascade
// execution seam.
#[test]
#[ignore]
fn handle_foreign_key_cascade_panic_surfaces_injected_error() {}

/// Go `pkg/executor/executor_failpoint_test.go:922::TestBuildProjectionForIndexJoinPanic`.
// go-parity-gap: the `buildProjectionForIndexJoinPanic` failpoint fires inside
// Go's index-join projection builder; this tier builds index-join projections
// in `crate::access_path` without an injectable failure point.
#[test]
#[ignore]
fn build_projection_for_index_join_panic_surfaces_injected_error() {}

/// Go `pkg/executor/executor_failpoint_test.go:1100::TestIndexLookUpPushDownExec`.
// go-parity-gap: pins the `index_lookup_pushdown` hint (`LocalIndexLookUp`
// plans, handle-filter injection through the
// `unistore/cophandler/inject-index-lookup-handle-filter` failpoint, actRows
// accounting across the local/remote split); the pushdown execution path,
// the hint, and the store failpoint are unported here.
#[test]
#[ignore]
fn index_lookup_pushdown_exec_matches_table_scan_results() {}

/// Go `pkg/executor/executor_failpoint_test.go:1152::
/// TestIndexLookUpPushDownPartitionExec`.
// go-parity-gap: the same pushdown verifier over partitioned tables
// (int/common/extra handles); requires the unported pushdown path and store
// failpoint, plus partition-aware LocalIndexLookUp planning.
#[test]
#[ignore]
fn index_lookup_pushdown_partition_exec_matches_table_scan_results() {}
