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

//! Port ledger for `pkg/planner/core/lateral_join_test.go` items 650–660 of
//! `pkg/planner.part11` on `origin/master` (test functions :30 … :536).
//!
//! Family contract: LATERAL derived tables build LogicalApply plans through
//! `BuildLogicalPlanForTest(ctx, s.GetSCtx(), nodeW, s.GetIS())` over
//! coretestsdk.CreatePlannerSuiteElems() fixtures; assertions inspect plan
//! trees with local helpers findLogicalApply/findFirstLogicalApply/
//! countLogicalApply (:573-626) and ToString rendering. JOIN-type gating and
//! scope rules live in PlanBuilder.buildSelect /
//! pkg/planner/core/logical_plan_builder.go:982-1008 (NATURAL/USING/LEFT/RIGHT
//! against the LATERAL table rejected via ErrInvalidLateralJoin,
//! plannererrors/planner_terror.go:99).
//!
//! All eleven items are honest gap ports: this crate has no SQL→plan builder
//! driven over parsed statements (plan_builder/from.rs covers fragments), no
//! DecorrelateSolver pipeline over built trees. Sibling file
//! lateral_join_source.rs already pins part12's :627/:790 items; these are
//! the remaining :30-:626 functions. Nothing was approximated.

/// GO PORT of `pkg/planner/core/lateral_join_test.go:30
/// TestLateralJoinPlanBuilding`.
///
/// Re-derived contract: eight cases (:36-71) — comma-LATERAL, CROSS JOIN
/// LATERAL, correlated LATERAL (`t.a = t1.a`), multiple chained LATERALs
/// (dt1 feeds dt2 shape), and aggregate+correlation all BUILD LogicalApply;
/// LEFT JOIN LATERAL ON true and RIGHT JOIN LATERAL ON true are REJECTED
/// (:43-52); a plain non-LATERAL derived table must NOT produce Apply
/// (:53-56). Each rejection parses fine — gating is semantic (:76-91).
#[test]
#[ignore = "go-parity-gap: needs BuildLogicalPlanForTest SQL->logical-plan pipeline + LogicalApply tree walk"]
fn lateral_join_plan_building_apply_matrix() {}

/// GO PORT of `pkg/planner/core/lateral_join_test.go:119
/// TestLateralJoinOptimization`.
///
/// Re-derived contract: three shapes build successfully with non-nil schema
/// (:131-140): uncorrelated LATERAL may decorrelate; correlated LATERAL
/// attempts decorrelation; correlated aggregate LATERAL stays an Apply. The
/// test itself only asserts NoError + NotNil(p.Schema()) (:126-142) — i.e.
/// none of them may fail plan construction.
#[test]
#[ignore = "go-parity-gap: needs BuildLogicalPlanForTest pipeline"]
fn lateral_join_optimization_shapes_build_with_schema() {}

/// GO PORT of `pkg/planner/core/lateral_join_test.go:159
/// TestLateralJoinReordering`.
///
/// Re-derived contract: LATERAL boundaries resist reordering — two chained
/// LATERALs keep at least TWO LogicalApply nodes in the optimized tree, one
/// LATERAL over two left tables keeps at least ONE (:164-202 via
/// countLogicalApply lower bounds :196-199).
#[test]
#[ignore = "go-parity-gap: needs plan building + apply-counting tree walk"]
fn lateral_join_reordering_preserves_apply_boundaries() {}

/// GO PORT of `pkg/planner/core/lateral_join_test.go:208
/// TestLateralJoinSchemaResolution`.
///
/// Re-derived contract: eleven all-success name-resolution cases (:214-264):
/// LATERAL reads left-side columns; outer WHERE references dt.b; nested
/// derived tables inside LATERAL; deep join trees (JOIN chains, USING(a)
/// merges, NATURAL joins) stay visible to LATERAL columns t1.c/t3.d; merged
/// USING/NATURAL column resolvable by table qualifier t2.a from LATERAL; a
/// second LATERAL sees the first LATERAL's output alias (dt1.a). Pins that
/// outer-scope visibility follows join-tree position, not textual order.
#[test]
#[ignore = "go-parity-gap: needs builder scope resolution incl. USING/NATURAL merge"]
fn lateral_join_schema_resolution_across_deep_trees() {}

/// GO PORT of `pkg/planner/core/lateral_join_test.go:294 TestLateralJoinExplain`.
///
/// Re-derived contract: `SELECT * FROM t, LATERAL (SELECT t.a) AS dt` builds
/// cleanly, renders a non-empty ToString plan, and the rendered/tree form
/// contains a LogicalApply operator (:302-315).
#[test]
#[ignore = "go-parity-gap: needs plan building + ToString renderer"]
fn lateral_join_explain_contains_apply_operator() {}

/// GO PORT of `pkg/planner/core/lateral_join_test.go:319
/// TestLateralJoinErrorPaths`.
///
/// Re-derived contract: five JOIN-kind cases (:325-359): RIGHT JOIN LATERAL
/// and LEFT JOIN LATERAL are errors (build-time, not parse-time; the case
/// table leaves expectedErrorCode 0 so only Error is asserted :361-381);
/// CROSS JOIN LATERAL, INNER JOIN LATERAL ON true, and comma-LATERAL build.
/// Message sources for the same gates:
/// logical_plan_builder.go:1008 ("LEFT JOIN is not supported with LATERAL")
/// and :802-812 neighborhood for RIGHT/USING/NATURAL variants.
#[test]
#[ignore = "go-parity-gap: needs builder JOIN-type gating vs ErrInvalidLateralJoin"]
fn lateral_join_error_paths_left_right_rejected_cross_inner_ok() {}

/// GO PORT of `pkg/planner/core/lateral_join_test.go:380
/// TestLateralJoinEdgeCases`.
///
/// Re-derived contract: constant subquery LATERAL (SELECT 1), always-false
/// inner WHERE (SELECT t.a WHERE false), UNION inside LATERAL ((SELECT t.a)
/// UNION (SELECT t.b)), and multi-column LATERAL payloads all BUILD without
/// error (:386-419).
#[test]
#[ignore = "go-parity-gap: needs BuildLogicalPlanForTest pipeline"]
fn lateral_join_edge_cases_build_cleanly() {}

/// GO PORT of `pkg/planner/core/lateral_join_test.go:430
/// TestLateralJoinWithAggregates`.
///
/// Re-derived contract: correlated COUNT(*), SUM, GROUP BY, MAX/MIN inside
/// LATERAL each still produce a LogicalApply in the built tree (:434-468),
/// pinning that aggregate-correlated laterals never degrade to plain joins
/// at build time.
#[test]
#[ignore = "go-parity-gap: needs aggregate-in-lateral planning over LogicalApply"]
fn lateral_join_correlated_aggregates_stay_as_apply() {}

/// GO PORT of `pkg/planner/core/lateral_join_test.go:476
/// TestLateralJoinComplexScenarios`.
///
/// Re-derived contract: real-world patterns build with non-nil schema
/// (:480-512): AVG over nested per-group COUNT inside LATERAL (aggregate of
/// aggregate through a subquery), multiple comma-LATERALs both referencing
/// t1, and compound OR/AND correlation conditions (t.a=t1.a AND t.b>t1.b OR
/// t.c<t1.c).
#[test]
#[ignore = "go-parity-gap: needs BuildLogicalPlanForTest pipeline"]
fn lateral_join_complex_scenarios_wellformed_plans() {}

/// GO PORT of `pkg/planner/core/lateral_join_test.go:517
/// TestLateralJoinScopeIsolationForNonLateralDerivedTable`.
///
/// Re-derived contract: adding a LATERAL sibling does NOT extend a plain
/// derived table's visibility: `FROM t AS t1 JOIN ((SELECT t1.a) AS s JOIN
/// LATERAL (SELECT 1) AS l ON true) ON true` errors with
/// "Unknown column 't1.a' in 'field list'" (:522-529) — non-LATERAL derived
/// tables keep capturing only their own scope even beside a LATERAL.
#[test]
#[ignore = "go-parity-gap: needs builder scope capture rules + unknown-column error surface"]
fn non_lateral_derived_table_stays_scope_isolated_beside_lateral() {}

/// GO PORT of `pkg/planner/core/lateral_join_test.go:536
/// TestLateralJoinDecorrelateWithUSINGAndON`.
///
/// Re-derived contract: with the outer side wrapped by LogicalSelection (ON
/// clauses around a USING(a) merge), DecorrelateSolver.Optimize (:555-571
/// driving pkg/planner/core/rule_decorrelate.go semantics) must still see
/// CorCols for the merged column reference t4.a=t2.a — the plan SURVIVES as
/// a LogicalApply with len(CorCols) > 0 instead of being wrongly rewritten
/// to a plain Join (:565-570 via findFirstLogicalApply). The fix context:
/// CorCols came out empty when Selection wrappers sat between Apply root and
/// the USING-merged join, producing wrong results.
#[test]
#[ignore = "go-parity-gap: needs built USING-merge plans + DecorrelateSolver over real trees"]
fn decorrelate_solver_keeps_apply_for_using_merged_cor_col() {}
