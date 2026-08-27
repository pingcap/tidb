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

//! Documentary gap ports for `pkg/planner/core/casetest/windows/`
//! (`pkg/planner.part10` items 550-556 on `origin/master`): `main_test.go`
//! TestMain, `window_push_down_test.go` and `window_with_exist_subquery_test.go`.
//! Every functional body runs through
//! `testkit.RunTestUnderCascades(WithDomain)` with a virtual TiFlash replica
//! and replays plan_tree/plan + warning goldens from the window push-down
//! suite book — the session/Domain stack this workspace does not have yet.

/// GO PORT of `pkg/planner/core/casetest/windows/main_test.go:29 TestMain`.
///
/// Bootstrap only: loads the window push-down suite books and wraps in goleak;
/// no assertions of its own.
#[test]
#[ignore = "go-parity-gap: bootstrap harness (suite books + goleak), no Rust twin by design"]
fn main_loads_window_pushdown_suite_books() {}

/// GO PORT of
/// `pkg/planner/core/casetest/windows/window_push_down_test.go:57
/// TestWindowFunctionDescCanPushDown`.
///
/// Contract (:57-69): over employee(empid, deptid, salary) with a virtual
/// TiFlash replica, each suite query's plan rows and warnings replay exactly —
/// pinning which WindowFuncDesc shapes CAN push down to TiFlash (per-window
/// push-down decisions render into the golden plans).
#[test]
#[ignore = "go-parity-gap: TiFlash replica injection plus window push-down goldens need the unported session pipeline"]
fn window_function_desc_can_push_down_goldens() {}

/// GO PORT of
/// `pkg/planner/core/casetest/windows/window_push_down_test.go:72
/// TestWindowPushDownPlans`.
///
/// Same fixture as :57; the suite's plan goldens pin whole pushed-down plan
/// SHAPES (exchange operators included) rather than just admissibility.
#[test]
#[ignore = "go-parity-gap: MPP-shaped window plans need the executor/Domain tier"]
fn window_push_down_plan_shapes_match_golden() {}

/// GO PORT of
/// `pkg/planner/core/casetest/windows/window_push_down_test.go:87
/// TestWindowPlanWithOtherOperators`.
///
/// Extends :72 with a second fixture pair t1/t2 (varchar/datetime/bigint),
/// `tidb_enforce_mpp=1`, and replicas on both tables (:105-110); goldens pin
/// windows coexisting with joins/aggregations/sorts under enforced MPP.
#[test]
#[ignore = "go-parity-gap: enforced-MPP multi-operator planning needs session plumbing"]
fn window_plans_with_other_operators_under_enforced_mpp() {}

/// GO PORT of
/// `pkg/planner/core/casetest/windows/window_with_exist_subquery_test.go:24
/// TestWindowSubqueryRewrite`.
///
/// Contract (:24-90): an EXISTS subquery whose SELECT carries FIRST_VALUE/MIN
/// OVER a named WINDOW whose PARTITION BY contains ANOTHER EXISTS-subquery
/// returns "1" for both the correlated-partition form and the plain-column
/// partition form (:45-84); then `count(1 in (select ...)) over ()` and
/// `count(1 = any (select ...) ) over ()` return one row per outer row
/// (:85-89) — window-aggregated subquery predicates keep their result
/// multiplicity.
#[test]
#[ignore = "go-parity-gap: window-over-subquery execution semantics need the executor"]
fn window_subquery_rewrite_executes_correlated_partition_forms() {}

/// GO PORT of
/// `pkg/planner/core/casetest/windows/window_with_exist_subquery_test.go:91
/// TestWindowSubqueryOuterRef`.
///
/// Contract (:91-122): every suite SQL is planned AND executed — its EXPLAIN
/// FORMAT='plan_tree' rows and its RESULT rows must both equal the recorded
/// values — pinning that outer references reaching window PARTITION BY via
/// subqueries keep plan shape AND results stable.
#[test]
#[ignore = "go-parity-gap: dual plan+result golden checking needs sessions"]
fn window_subquery_outer_ref_plan_and_result_goldens() {}

/// GO PORT of
/// `pkg/planner/core/casetest/windows/window_with_exist_subquery_test.go:123
/// TestWindowWithOuterJoinAndCTE`.
///
/// Contract (:123-160): clustered-PK t0/t1 fixtures; every suite query's
/// plan_tree rows and execution results replay exactly — windows OVER OUTER
/// JOIN + CTE combinations stay executable per the recorded outputs.
#[test]
#[ignore = "go-parity-gap: CTE+outer-join window execution needs the unported engine"]
fn window_with_outer_join_and_cte_plan_and_result_goldens() {}
