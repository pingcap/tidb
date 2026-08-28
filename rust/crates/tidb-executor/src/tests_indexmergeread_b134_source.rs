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

//! Ports of Go `pkg/executor/test/indexmergereadtest/index_merge_reader_test.go`
//! items 924–944.
//!
//! The Rust executor already has dependency-closed algebra tests alongside
//! `index_merge_reader.rs`. These source-mapped tests remain explicit gaps for
//! the Go cases whose assertions require the SQL planner, coprocessor workers,
//! failpoints, partition-region metadata, or session process information.

/// Go `TestIndexMergePickAndExecTaskPanic` (:39): SQL index-merge task panic
/// failpoint and error propagation.
#[test]
#[ignore = "go-parity-gap: SQL index-merge worker failpoint is unported"]
fn index_merge_pick_and_exec_task_panic() {}

/// Go `TestPartitionTableRandomIndexMerge` (:58): randomized partition-table
/// index-merge results match a normal table scan.
#[test]
#[ignore = "go-parity-gap: partition pruning + SQL index-merge planner are unported"]
fn partition_table_random_index_merge() {}

/// Go `TestPartitionTableRandomIndexMerge2` (:95): primary-key partition-table
/// randomized index-merge equivalence.
#[test]
#[ignore = "go-parity-gap: partition pruning + SQL index-merge planner are unported"]
fn partition_table_random_index_merge_with_primary_key() {}

/// Go `TestIndexMergeWithPreparedStmt` (:133): prepared plan-cache execution
/// and process EXPLAIN expose an IndexMerge plan.
#[test]
#[ignore = "go-parity-gap: prepared statements, plan cache, and process EXPLAIN are unported"]
fn index_merge_with_prepared_stmt() {}

/// Go `TestMVIndexMergePlanTree` (:167): multi-valued-index member-of plan
/// tree rendering.
#[test]
#[ignore = "go-parity-gap: multi-valued index and plan-tree rendering are unported"]
fn mv_index_merge_plan_tree() {}

/// Go `TestIndexMergeReaderMemTracker` (:189): index-merge read memory usage
/// and EXPLAIN ANALYZE memory output.
#[test]
#[ignore = "go-parity-gap: index-merge SQL memory accounting and EXPLAIN ANALYZE are unported"]
fn index_merge_reader_mem_tracker() {}

/// Go `TestPessimisticLockOnPartitionForIndexMerge` (:223): partitioned
/// index-merge SELECT FOR UPDATE lock interaction.
#[test]
#[ignore = "go-parity-gap: pessimistic transactions, locks, and partition SQL plans are unported"]
fn pessimistic_lock_on_partition_for_index_merge() {}

/// Go `TestIndexMergeIntersectionConcurrency` (:303): intersection worker
/// concurrency is controlled by session variables and failpoints.
#[test]
#[ignore = "go-parity-gap: concurrent index-merge workers and failpoints are unported"]
fn index_merge_intersection_concurrency() {}

/// Go `TestIntersectionWithDifferentConcurrency` (:361): dynamic/static
/// partition pruning and repeated transactional intersection equivalence.
#[test]
#[ignore = "go-parity-gap: partition pruning, transactions, and SQL index-merge plans are unported"]
fn intersection_with_different_concurrency() {}

/// Go `TestIntersectionWorkerPanic` (:448): intersection worker panic
/// failpoint reaches the client error.
#[test]
#[ignore = "go-parity-gap: SQL index-merge worker failpoint is unported"]
fn intersection_worker_panic() {}

/// Go `TestIndexMergeProcessWorkerHang` (:481): union/intersection worker
/// early-return and hang failpoints are cleaned up and reported.
#[test]
#[ignore = "go-parity-gap: asynchronous index-merge worker lifecycle is unported"]
fn index_merge_process_worker_hang() {}

/// Go `TestIndexMergePanic` (:528): all partial/process/table worker panic
/// paths and result-channel closure behavior.
#[test]
#[ignore = "go-parity-gap: asynchronous index-merge worker panic paths are unported"]
fn index_merge_panic() {}

/// Go `TestIndexMergeError` (:590): partial worker errors propagate through
/// repeated SQL execution.
#[test]
#[ignore = "go-parity-gap: asynchronous index-merge worker error paths are unported"]
fn index_merge_error() {}

/// Go `TestIndexMergeCoprGoroutinesLeak` (:610): coprocessor goroutine leak
/// failpoints are observed by the test.
#[test]
#[ignore = "go-parity-gap: coprocessor goroutine lifecycle is unported"]
fn index_merge_copr_goroutines_leak() {}

/// Go `TestOrderByWithLimit` (:657): index-merge ORDER BY/LIMIT result and
/// plan equivalence across handle and partitioning modes.
#[test]
#[ignore = "go-parity-gap: SQL index-merge ORDER BY/LIMIT planner and partition modes are unported"]
fn order_by_with_limit() {}

/// Go `TestProcessInfoRaceWithIndexScan` (:771): process-info logging races
/// with repeated index scans.
#[test]
#[ignore = "go-parity-gap: process information and concurrent SQL execution are unported"]
fn process_info_race_with_index_scan() {}

/// Go `TestIndexMergeReaderIssue45279` (:802): cancellation during index
/// merge returns successfully without leaking the worker.
#[test]
#[ignore = "go-parity-gap: SQL query cancellation and worker lifecycle are unported"]
fn index_merge_reader_issue45279() {}

/// Go `TestIndexMergeLimitPushedAsIntersectionEmbeddedLimit` (:825): embedded
/// intersection LIMIT returns the same row count as a table scan.
#[test]
#[ignore = "go-parity-gap: SQL index-merge LIMIT embedding and planner keyword output are unported"]
fn index_merge_limit_pushed_as_intersection_embedded_limit() {}

/// Go `TestIndexMergeLimitNotPushedOnPartialSideButKeepOrder` (:849): forced
/// keep-order partial-side LIMIT/OFFSET matches the normal index plan.
#[test]
#[ignore = "go-parity-gap: SQL keep-order index-merge planner and failpoint are unported"]
fn index_merge_limit_not_pushed_on_partial_side_but_keep_order() {}

/// Go `TestIssues46005` (:895): index-merge ORDER BY LIMIT 1025 remains
/// correct when lookup batches are 1024 rows.
#[test]
#[ignore = "go-parity-gap: SQL index-merge lookup batching and ORDER BY planner are unported"]
fn issues46005() {}

/// Go `pkg/executor/test/indexmergereadtest/main_test.go:26::TestMain`:
/// package-level goleak/configuration bootstrap, not product behavior.
#[test]
#[ignore = "skipped-reason: Go suite bootstrap/goleak has no Rust test behavior"]
fn index_merge_suite_main_is_bootstrap() {}
