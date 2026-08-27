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

//! Port ledger for `pkg/planner/core/casetest/instanceplancache/
//! concurrency_test.go` plus `concurrency_tpcc_test.go` (`pkg/planner.part5`,
//! items 249–260 of all `Test*`/`Benchmark*` declarations under
//! `pkg/planner/` on `origin/master`, sorted by file path then line).
//!
//! Shared methodology (concurrency_test.go:30-47): N sessions share one
//! instance-wide plan cache (`tidb_enable_instance_plan_cache=1`); each stmt
//! exists in two forms — a literal "normal" SQL run against a `normal`
//! database and a prepared-format triple (prepare/set/execute) against an
//! identical `prepared` database — and every worker must observe IDENTICAL
//! results from both forms inside one transaction. DML statements are
//! partitioned across workers to avoid duplicated-row deadlocks
//! (concurrency_test.go:64-79 `worker.run`, deadlock-tolerant skip).
//!
//! All twelve items are honest gap ports: they need the mock store, real
//! sessions with PREPARE/EXECUTE protocol, transaction semantics, and the
//! domain-scoped instance plan cache — none of which this crate carries.

/// GO PORT of `concurrency_test.go:123 TestInstancePlanCacheConcurrencySysbench`.
///
/// Re-derived contract: sysbench-style sbtest(id auto-inc PK, k indexed,
/// c char(120)) in two databases; 2000-stmt mix generated per Go ratios
/// (:166-198: ~50% DQL of five SELECT shapes over id point/between,
/// sum(k), order-by c, distinct; 25% UPDATE k+1 / c; 15% INSERT; 10%
/// DELETE; new txn every ~15 stmts) spread over 10 workers; DQL rows from
/// the normal database must equal prepared-database rows statement-for-
/// statement across all cached executions (:48-58).
#[test]
#[ignore = "go-parity-gap: needs mock store + multi-session workload + txn + instance plan cache"]
fn instance_plan_cache_concurrency_sysbench_normal_vs_prepared_parity() {}

/// GO PORT of `concurrency_test.go:293 TestInstancePlanCacheIndexJoin`.
///
/// Re-derived contract: t1(a,b) ×100 rows, t2(a,key); ten goroutines share
/// ONE prepared `select /*+ tidb_inlj(t2) */ t2.a from t1,t2 where
/// t1.a=t2.a and t1.b=?`; executing with @v must always return exactly the
/// single row `v` even when the index-join plan was concurrently built or
/// reused by other sessions — pins parameter isolation inside shared
/// index-join plans.
#[test]
#[ignore = "go-parity-gap: needs mock store + session PREPARE/EXECUTE + instance plan cache"]
fn instance_plan_cache_index_join_shared_plan_parameter_isolation() {}

/// GO PORT of `concurrency_test.go:322 TestInstancePlanCacheTableIndexScan`.
///
/// Re-derived contract: t(a PK, b key) ×100; each round RE-PREPARES either
/// the primary-key table scan (`use index(primary)` on a-range) or the
/// secondary index scan (`use index(b)` on b-range), binds [v1∈[0,50),
/// v2∈[50,100)] and expects exactly integers v1..v2 sorted; pins that
/// re-preparing alternates access paths without cross-contaminating the
/// shared cache entry's ranges.
#[test]
#[ignore = "go-parity-gap: needs mock store + session PREPARE/EXECUTE + instance plan cache"]
fn instance_plan_cache_table_index_scan_alternating_paths_stay_correct() {}

/// GO PORT of `concurrency_test.go:361 TestInstancePlanCacheConcurrencyPointPartitioning`.
///
/// Re-derived contract: hash-partitioned t1(10 parts) and range-partitioned
/// t2(10 parts), both INT PK ×100 rows; ten workers repeatedly prepare and
/// execute `select * from <t> where a=?` against random partitions; each hit
/// returns exactly the single matching row — point get through dynamic
/// partition pruning must be cacheable without partition-parameter leakage.
#[test]
#[ignore = "go-parity-gap: needs mock store + partitioned tables + instance plan cache"]
fn instance_plan_cache_point_partitioning_hash_and_range_pruned_hits() {}

/// GO PORT of `concurrency_test.go:402 TestInstancePlanCacheConcurrencyPointMultipleColPKNoTxn`.
///
/// Re-derived contract: composite PK (a,b) table; one shared prepare of
/// `select * from t where a=? and b=?` probed with a=b=v ∈ [0,100); every
/// execution (first = compile+populate, rest = cache hits) returns exactly
/// row `v v`.
#[test]
#[ignore = "go-parity-gap: needs mock store + session PREPARE/EXECUTE + instance plan cache"]
fn instance_plan_cache_point_multi_column_pk_no_txn_hits() {}

/// GO PORT of `concurrency_test.go:430 TestInstancePlanCacheConcurrencyPointNoTxn`.
///
/// Re-derived contract: single-column PK table; shared prepare
/// `select * from t where a=?` executed autocommit by ten workers; result
/// set for @a=v is exactly `v v` on every execution including concurrent
/// cache hits.
#[test]
#[ignore = "go-parity-gap: needs mock store + session PREPARE/EXECUTE + instance plan cache"]
fn instance_plan_cache_concurrency_point_no_txn_hits() {}

/// GO PORT of `concurrency_test.go:458 TestInstancePlanCacheBatchPointMultiColIndex`.
///
/// Re-derived contract: composite PK (a,b) AND unique key (c,d) table; per
/// round a fresh prepare picks `(a,b) in ((?,?),(?,?))` or the `(c,d)`
/// variant; binding p1=a1,p2=a1,p3=a2,p4=a2 must return exactly {a1,a2}
/// sorted — batch point get over two-column indexes stays correct under
/// shared-cache reuse for either key path.
#[test]
#[ignore = "go-parity-gap: needs mock store + session PREPARE/EXECUTE + instance plan cache"]
fn instance_plan_cache_batch_point_multi_col_index_either_key_path() {}

/// GO PORT of `concurrency_test.go:494 TestInstancePlanCacheConcurrencyBatchPointNoTxn`.
///
/// Re-derived contract: `select a from t where a in (?, ?)` with a1∈[0,50),
/// a2∈[50,100); every shared-plan execution returns exactly {a1,a2} sorted.
#[test]
#[ignore = "go-parity-gap: needs mock store + session PREPARE/EXECUTE + instance plan cache"]
fn instance_plan_cache_concurrency_batch_point_no_txn_hits() {}

/// GO PORT of `concurrency_test.go:527 TestInstancePlanCacheConcurrencyPoint`.
///
/// Re-derived contract: t1(col1 PK, col2 unique) mirrored into normal/
/// prepared databases; 400-stmt stream of two point-select shapes run via
/// the testWithWorkers harness (:80-121) — every DQL answer from the plain
/// form equals the prepared form inside each worker txn.
#[test]
#[ignore = "go-parity-gap: needs mock store + multi-session workload + txn + instance plan cache"]
fn instance_plan_cache_concurrency_point_normal_vs_prepared_parity() {}

/// GO PORT of `concurrency_test.go:585 TestInstancePlanCacheConcurrencyPartitioning`.
///
/// Re-derived contract: range-partitioned t over [0,100) in 10 parts; three
/// re-prepared shapes per round — point get (single row), IN-list of two
/// values (sorted pair), BETWEEN range (v1..v2 inclusive sorted) — all
/// return live results under ten-way concurrency; pins that partition
/// pruning inputs are re-derived from bound parameters at execution time,
/// never frozen in the shared plan.
#[test]
#[ignore = "go-parity-gap: needs mock store + partitioned tables + instance plan cache"]
fn instance_plan_cache_concurrency_partitioning_three_shapes() {}

/// GO PORT of `concurrency_test.go:645 TestInstancePlanCacheConcurrencyComp`.
///
/// Re-derived contract: compatibility suite from PingCAP-QE/qa comp cases:
/// t1(col1,col2,key(col1,col2)) mirrored normal/prepared; generators emit
/// INSERT plus a 2×3 grid of point predicates (=, IS NULL, IN 3-list) and
/// range predicates (BETWEEN, >, <=) over col1; normal results must equal
/// prepared results for every shape under shared-cache reuse.
#[test]
#[ignore = "go-parity-gap: needs mock store + multi-session workload + txn + instance plan cache"]
fn instance_plan_cache_concurrency_comp_shapes_parity() {}

/// GO PORT of `concurrency_tpcc_test.go:25 TestInstancePlanCacheConcurrencyTPCC`.
///
/// Re-derived contract: TPCC-shaped warehouse/district/customer/item/stock
/// schema (prepareTPCC helper); NewOrder transactions interleave a
/// customer-warehouse join select, a FOR UPDATE district read, a district
/// next_o_id update, and stock updates while Payment interleaves
/// d_ytd/w_ytd balance updates — each statement re-prepared per round;
/// prepared-form execution must leave both databases' visible state equal
/// to the plain-SQL form after concurrent commit cycles (:153-190 workers).
#[test]
#[ignore = "go-parity-gap: needs mock store + TPCC fixture + multi-session txns + instance plan cache"]
fn instance_plan_cache_concurrency_tpcc_workload_state_parity() {}
