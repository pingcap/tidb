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

//! Port ledger for `pkg/planner/core/casetest/instanceplancache/dml_test.go`
//! and its `main_test.go` driver (`pkg/planner.part5`, items 261–265 of all
//! `Test*`/`Benchmark*` declarations under `pkg/planner/` on `origin/master`,
//! sorted by file path then line).
//!
//! Family contract: prepared-form DML (INSERT/UPDATE/DELETE through
//! prepare/set/execute) must leave table state byte-for-byte equal to the
//! same DML written with inline literals — including when the plan comes
//! from the shared instance plan cache (`@@last_plan_from_cache=1`), across
//! range/hash/list-columns partitioned tables and explicit
//! `PARTITION(p)` targets.
//!
//! All five items are honest gap ports: they need the mock store, session
//! PREPARE/EXECUTE protocol, DML executors, and the domain-scoped instance
//! plan cache; this crate carries none of these.

/// GO PORT of `dml_test.go:110 TestInstancePlanCacheDMLTPCC`.
///
/// Re-derived contract: TPCC warehouse/district/customer tables (100 rows
/// each) created identically in databases tpcc1 and tpcc2; randomized
/// payment updates (`d_ytd = d_ytd + ?`, `w_ytd = w_ytd + ?`,
/// `c_balance = c_balance - ?`, data-update variant) run in prepared form
/// against tpcc1 and literal form against tpcc2; after each batch
/// `select * from <table>` over both databases sorts equal (:175 check()).
#[test]
#[ignore = "go-parity-gap: needs mock store + session PREPARE/EXECUTE + DML executors + instance plan cache"]
fn instance_plan_cache_dml_tpcc_prepared_equals_literal_state() {}

/// GO PORT of `dml_test.go:241 TestInstancePlanCacheDMLBasic`.
///
/// Re-derived contract: t1 (indexed) receives INSERT `(?, ?, ?)` ×50 then
/// 100 rounds of random INSERT / `delete ... where a<? and b<? or c<?` /
/// `update ... set a=? where b=? or c=?` in prepared form while the identical
/// literal statement runs on t2; after EVERY pair both tables sort equal
/// (:289-330 checkResult) — pins predicate-shape fidelity of cached DML
/// plans under OR-of-range conditions.
#[test]
#[ignore = "go-parity-gap: needs mock store + session PREPARE/EXECUTE + DML executors + instance plan cache"]
fn instance_plan_cache_dml_basic_prepared_matches_literal_rows() {}

/// GO PORT of `dml_test.go:313 TestInstancePlanCacheUpdateSpecifiedPartition`.
///
/// Re-derived contract: two identical range-partitioned t1/t2 (p0..p3,
/// values <10/20/30/40); 100 rounds either update partition pIdx explicitly
/// (`update t1 partition(p<v>) set b=b+?`) or the whole table; the prepared
/// form hits the instance cache every time (`last_plan_from_cache=1`,
/// :329/:338) yet mutates exactly the rows the literal form on t2 does —
/// explicit-partition DML plans are cacheable without freezing the
/// partition target or the increment parameter.
#[test]
#[ignore = "go-parity-gap: needs mock store + partitioned-table UPDATE executor + instance plan cache"]
fn instance_plan_cache_update_specified_partition_hits_cache_and_mutates_correctly() {}

/// GO PORT of `dml_test.go:352 TestInstancePlanCacheDMLPartitioning`.
///
/// Re-derived contract: range(t1)/hash(t2)/list-columns(t3) partitioned
/// tables each exercise five prepared DML shapes — bare insert, column-list
/// insert, unconditional delete, `where a=?` delete, two-column-eq delete —
/// executed twice per prepare so the second run is a cache hit asserted via
/// `@@last_plan_from_cache=1`; pins that INSERT row routing and DELETE
/// pruning all re-bind from parameters under the instance cache.
#[test]
#[ignore = "go-parity-gap: needs mock store + partitioned-table DML executors + instance plan cache"]
fn instance_plan_cache_dml_partitioning_three_schemes_five_shapes() {}

/// GO PORT of `main_test.go:174 TestInstancePlanCache`.
///
/// Re-derived contract: ten randomized tables (6 columns mixed int/
/// varchar/float/double/datetime with PK/index subsets, 100 rows,
/// main_test.go:139-160 prepareTables) are probed by ~190 generated query
/// patterns (:302+ init(): single-filter selects, ORDER BY/LIMIT composites,
/// multi-condition ANDs, IN-lists) ; each pattern becomes one PREPARE plus
/// 5 random parameter sets, and FIVE concurrent workers
/// (executeWorker :187-210) each execute the plain SELECT once and the
/// EXECUTE form three times demanding sorted equality every time — proving
/// results never drift between fresh compiles and repeated instance-cache
/// hits for arbitrary value/type combinations.
#[test]
#[ignore = "go-parity-gap: needs mock store + randomized cross-session workload harness + instance plan cache"]
fn instance_plan_cache_randomized_cross_session_query_pattern_parity() {}
