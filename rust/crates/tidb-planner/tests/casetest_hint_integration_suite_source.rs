// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Documentary gap ports for `pkg/planner/core/casetest/hint`
//! (`pkg/planner.part4` items 199–207 on `origin/master`).
//!
//! Eight of the nine Go tests are golden books over live mock-store
//! sessions: every input runs through `testKit.MustQuery` and pins both its
//! plan rows and its converted `SQLWarn` strings from the
//! `integration_suite` book, several with hacked available TiFlash replicas
//! (`testkit.SetTiFlashReplica` or direct `tbl.Meta().TiFlashReplica`
//! assignment) plus views/CTEs. None of that machinery is reachable from
//! this crate. The ninth — `TestOptimizerCostFactorHints` (:360) — has a
//! REAL partial functional port in the sibling module
//! [`hint_optimizer_cost_factor_setvar_scenarios_source.rs`], which pins the
//! per-operator cost-factor mapping through this crate's transcreated
//! `plan_cost_ver2` primitives; only its SET_VAR/explain end-to-end surface
//! stays a gap there. The bootstrap `hint/main_test.go:30 TestMain` is
//! skipped-reason: loads the integration_suite book, zeroes async-commit
//! clock-drift config, goleak.

/// GO PORT of `pkg/planner/core/casetest/hint/hint_test.go:35
/// TestReadFromStorageHint`.
///
/// Re-derived contract: over t(a,b,index ia(a)), tt(a pk), ttt(a desc pk)
/// each carrying an available TiFlash replica, with `tidb_allow_mpp=OFF` and
/// `tidb_allow_tiflash_cop=ON`, `read from tiflash/tikv` storage hints must
/// route each query's scans to the hinted engine; plan AND warning lists are
/// pinned per case.
#[test]
#[ignore = "go-parity-gap: needs TiFlash-replica meta injection plus live explain/warning goldens"]
fn read_from_storage_hint_routes_scans_golden() {}

/// GO PORT of `pkg/planner/core/casetest/hint/hint_test.go:72
/// TestAllViewHintType`.
///
/// With `tidb_isolation_read_engines='tiflash, tikv'`, a replica on t only,
/// and views v…v12 covering plain joins, nested-view joins, aggregates over
/// subqueries, EXISTS/scarar-subquery bodies, CTE + UNION mixes,
/// sum/group-by/limit/order shapes and self-joins, hints written on each
/// view body must still land in the expanded plan; golden plans + warnings.
#[test]
#[ignore = "go-parity-gap: view-column expansion plus hint binding through nested view bodies are unported"]
fn all_view_hint_types_apply_through_twelve_view_shapes() {}

/// GO PORT of `pkg/planner/core/casetest/hint/hint_test.go:131
/// TestJoinHintCompatibility`.
///
/// Views v (leading(t1)+inl_join(t1)), v1 (leading(t2)+merge_join(t)) and v2
/// (hint-free) over t..t9 — three of them hash-partitioned 4 ways and
/// analyzed, replicas on t4-t6 — pin which join hints survive outer queries
/// combining old/new hint syntax across views; plans + warnings are golden.
#[test]
#[ignore = "go-parity-gap: hint compatibility matrix spans view/predicate/hint interplay the crate cannot execute"]
fn join_hint_compatibility_matrix_golden() {}

/// GO PORT of `pkg/planner/core/casetest/hint/hint_test.go:180
/// TestReadFromStorageHintAndIsolationRead`.
///
/// t(a int, b int, index ia(a)) with a replica while the session pins
/// `tidb_isolation_read_engines="tikv"`: a read-from-tiflash hint must NOT
/// escape the isolation allow-list; exact plan rows and warnings recorded.
#[test]
#[ignore = "go-parity-gap: isolation-read engine filtering has no standalone owner here"]
fn read_from_storage_hint_yields_to_isolation_read_engines() {}

/// GO PORT of `pkg/planner/core/casetest/hint/hint_test.go:211
/// TestIsolationReadTiFlashUseIndexHint`.
///
/// Vector column setup (vector(3) col + clustered PK + vector index via HNSW
/// with failpoint MockCheckColumnarIndexProcess and a mock TiFlash server),
/// isolation tiflash: index hints steering between plain idx and vecIdx must
/// produce the book plans without falling back to tikv.
#[test]
#[ignore = "go-parity-gap: vector/HNSW index planning path and mock-TiFlash topology are unported"]
fn isolation_read_tiflash_respects_use_index_hint_on_vector_table() {}

/// GO PORT of `pkg/planner/core/casetest/hint/hint_test.go:253
/// TestOptimizeHintOnPartitionTable`.
///
/// Range-columns partitioned t under `tidb_partition_prune_mode='static'`,
/// index-merge disabled, replica attached by direct meta write:
/// `explain format='plan_tree' <input>` rows AND matching
/// `show warnings` rows are pinned pairwise. The tail (:295-303) additionally
/// pins MAX_EXECUTION_TIME(10) hint behavior: SLEEP(5) returns "0" unhinted;
/// with dtc(name=tt) exactly one warning accumulates; adding unknown(t1,t2)
/// raises two.
#[test]
#[ignore = "go-parity-gap: static-partition-prune planning plus MAX_EXECUTION_TIME/dtc warning counters need a live session"]
fn optimize_hints_on_partition_table_plans_and_warnings_golden() {}

/// GO PORT of `pkg/planner/core/casetest/hint/hint_test.go:307 TestHints`.
///
/// Broad hint grammar sweep over t1/t2/t3: for each input SQL the paired
/// `explain format='plan_tree'` output and `show warnings` output are
/// asserted verbatim against the book — the catch-all regression net for
/// optimizer-hint parsing/binding drift.
#[test]
#[ignore = "go-parity-gap: golden sweeps over live explain trees"]
fn hints_plan_tree_and_warning_pairs_golden() {}

/// GO PORT of `pkg/planner/core/casetest/hint/hint_test.go:333
/// TestQBHintHandlerDuplicateObjects`.
///
/// t_employees(id pk auto_increment, fname, lname, store_id,
/// department_id, index idx(department_id)): when one object name appears
/// twice in a hint list, the QB hint handler must resolve duplicates the way
/// the book records (plans + warnings per input).
#[test]
#[ignore = "go-parity-gap: QBHintHandler duplicate-object resolution runs inside the unported hint-binding pass"]
fn qb_hint_handler_duplicate_objects_golden() {}
