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

//! Ports for `pkg/planner/core/tests/pointget/point_get_plan_test.go`
//! items 1141–1143 of `pkg/planner.part20` (all 1278 `Test*`/`Benchmark*`
//! declarations under `pkg/planner/` on `origin/master`, sorted by file then
//! line, chunked by 60; part19 ended at `:342 TestIssue18042`). The earlier
//! families of this file live in `tests_pointget_plan_cache_source.rs`
//! (part19); these three are the tail.
//!
//! All three Go tests drive `testkit` over a mock store: DDL + DML, then
//! either `explain format = 'plan_tree'` goldens or an UPDATE plus SELECT
//! round-trip. The crate has no session/executor surface (the boundary every
//! neighbouring planner-part receipt records), so they are documentary
//! `#[ignore]` ports; nothing is approximated into passing.

/// GO PORT of `pkg/planner/core/tests/pointget/point_get_plan_test.go:355
/// TestIssue52592` (classic kernel: skipped under next-gen at :356-358).
///
/// Contract: on `t(a bigint unsigned primary key, b int, c int,
/// key idx_bc(b,c))` seeded with (1,1,1),(2,2,2),(3,3,3), with
/// `@@tidb_opt_fix_control = "52592:OFF"` the point-get fast path is kept —
/// `select * from t where a = 1` and the literal-swapped `where 1 = a` both
/// explain as `Point_Get root table:t handle:1` (:366-371), the UPDATE/DELETE
/// variants keep a `Point_Get` child under the write operator (:372-379),
/// and the unsigned out-of-domain literal `where a = -1` still folds to
/// `TableDual root rows:0` (:380-381). With `"52592:ON"` (:383) the fast
/// path is disabled: the same select/update/delete explain as
/// `TableReader root data:TableRangeScan` over
/// `TableRangeScan cop[tikv] table:t range:[1,1], keep order:false,
/// stats:pseudo` (:384-401), while `a = -1` remains `TableDual` (:402-403).
/// The gate lives in `TryFastPlan` (`pkg/planner/core/point_get_plan.go:
/// 83-86`: return nil when `fixcontrol.Fix52592` is on) and again at the
/// datasource fallback (`pkg/planner/core/find_best_task.go:2176-2178`:
/// `canConvertPointGet = false`).
///
/// go-parity-gap: the observable is explain-format physical plan output over
/// a live session; this crate has neither TryFastPlan nor an explain driver.
#[test]
#[ignore = "go-parity-gap: explain-format plan goldens need the session/Optimize/TryFastPlan + explain stack"]
fn issue_52592_fix_control_toggles_point_get_fast_path() {}

/// GO PORT of `pkg/planner/core/tests/pointget/point_get_plan_test.go:407
/// TestIssue52592ForNextGen` (next-gen kernel: skipped under classic at
/// :408-410).
///
/// Contract: identical schema and fix-control matrix as
/// `TestIssue52592`, but under next-gen the Update/Delete explain rows grow
/// lock operators — with `52592:OFF` the children read
/// `└─Point_Get root table:t handle:1, lock` (:424-431), and with
/// `52592:ON` a `└─SelectLock root  for update 0` is inserted between the
/// write operator and the TableReader (:444-455). `a = -1` still explains as
/// `TableDual root  rows:0` in both arms (:432-433, :456-458).
///
/// go-parity-gap: same session/explain boundary, plus Go's
/// `kerneltype.IsClassic`/`IsNextGen` gates have no Rust counterpart.
#[test]
#[ignore = "go-parity-gap: same explain/session boundary plus the kerneltype gate is unported"]
fn issue_52592_for_next_gen_lock_suffix_variant() {}

/// GO PORT of `pkg/planner/core/tests/pointget/point_get_plan_test.go:461
/// TestIssue56832`.
///
/// Contract: `t (id int primary key, c enum('0','1','2'))` seeded with
/// (0,'0'),(1,'1'),(2,'2') (:466-467); `update t set c = 2 where id = 0`
/// assigns the enum BY ORDINAL — value 2 is the second member `'1'` — so
/// `select c from t where id = 0` must return exactly `"1"` (:468-469).
/// Regression guard for issue 56832, where the int-to-enum assignment in
/// UPDATE lost the ordinal offset.
///
/// go-parity-gap: needs UPDATE execution over a table with an enum column
/// (executor + storage), which the crate does not own.
#[test]
#[ignore = "go-parity-gap: UPDATE/SELECT execution over an enum column needs the executor stack"]
fn issue_56832_update_enum_by_int_stores_nth_member() {}
